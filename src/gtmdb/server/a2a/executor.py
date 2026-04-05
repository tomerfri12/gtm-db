"""Run :class:`~gtmdb.analyst.runner.AnalystRunner` behind the A2A task protocol."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import Part, TextPart
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from fastapi import FastAPI

from gtmdb.analyst.runner import AnalystRunner, _extract_final_answer

log = logging.getLogger(__name__)


class GtmDBAnalystExecutor(AgentExecutor):
    """Streams analyst graph events as task artifacts; completes with the final answer."""

    def __init__(self, app: FastAPI) -> None:
        self._app = app

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        call = context.call_context
        scope = call.state.get("gtmdb_scope") if call else None
        task_id = context.task_id
        context_id = context.context_id
        if not task_id or not context_id:
            log.warning("[a2a] missing task_id or context_id")
            return
        updater = TaskUpdater(event_queue, task_id, context_id)

        if scope is None:
            await updater.failed(
                updater.new_agent_message(
                    parts=[
                        Part(
                            root=TextPart(
                                text="Unauthorized: missing scope (Bearer token required)."
                            )
                        )
                    ]
                )
            )
            return

        question = context.get_user_input().strip()
        if not question:
            await updater.failed(
                updater.new_agent_message(
                    parts=[Part(root=TextPart(text="Empty user message."))]
                )
            )
            return

        try:
            await updater.start_work()
            runner = AnalystRunner(
                self._app.state.db,
                scope=scope,
                tenant_id=scope.tenant_id,
            )
        except ValueError as e:
            await updater.failed(
                updater.new_agent_message(parts=[Part(root=TextPart(text=str(e)))])
            )
            return
        except Exception as e:
            log.exception("[a2a] failed to start analyst")
            await updater.failed(
                updater.new_agent_message(
                    parts=[Part(root=TextPart(text=f"Analyst error: {e!s}"))]
                )
            )
            return

        artifact_id = str(uuid.uuid4())
        step = 0
        tools_used = False
        messages: list[Any] = [HumanMessage(content=question)]
        final_messages: list[Any] = messages
        transcript_append = False

        async def _emit_event(payload: dict) -> None:
            nonlocal transcript_append
            await updater.add_artifact(
                parts=[
                    Part(
                        root=TextPart(
                            text=json.dumps(payload, ensure_ascii=False),
                            metadata={"gtmdb_analyst_event": payload.get("type")},
                        )
                    )
                ],
                artifact_id=artifact_id,
                name="analyst-events",
                append=transcript_append,
                last_chunk=False,
            )
            transcript_append = True

        try:
            async for event in runner._graph.astream(
                {"messages": messages},
                stream_mode="values",
            ):
                final_messages = event["messages"]
                last = final_messages[-1]

                if isinstance(last, AIMessage):
                    if last.tool_calls:
                        tools_used = True
                        tool_names = {tc["name"] for tc in last.tool_calls}
                        prose = str(last.content or "").strip()
                        if prose and "think" not in tool_names:
                            await _emit_event({"type": "plan", "text": prose})

                        for tc in last.tool_calls:
                            tool_name = tc["name"]
                            args = tc.get("args") or {}
                            if not isinstance(args, dict):
                                args = {}

                            if tool_name == "think":
                                plan_text = args.get("plan") or args.get("input") or ""
                                await _emit_event(
                                    {"type": "plan", "text": str(plan_text)},
                                )
                                continue

                            step += 1
                            query = args.get("query", "")
                            lang = (
                                "sql"
                                if tool_name == "execute_sql"
                                else "cypher"
                                if tool_name == "execute_cypher"
                                else "text"
                            )
                            await _emit_event(
                                {
                                    "type": "step",
                                    "step": step,
                                    "tool": tool_name,
                                    "lang": lang,
                                    "query": query,
                                },
                            )

                    elif last.content:
                        msg_type = "answer" if tools_used else "plan"
                        await _emit_event(
                            {"type": msg_type, "text": str(last.content)},
                        )

                elif isinstance(last, ToolMessage):
                    if last.name == "think":
                        continue
                    preview = last.content[:600]
                    await _emit_event(
                        {
                            "type": "result",
                            "step": step,
                            "text": preview,
                            "truncated": len(last.content) > 600,
                        },
                    )

            answer = _extract_final_answer(final_messages)
            await _emit_event({"type": "done"})
            await updater.complete(
                message=updater.new_agent_message(
                    parts=[Part(root=TextPart(text=answer))]
                )
            )

        except asyncio.CancelledError:
            await updater.cancel(
                message=updater.new_agent_message(
                    parts=[Part(root=TextPart(text="Canceled."))]
                )
            )
            raise
        except PermissionError as e:
            await updater.failed(
                updater.new_agent_message(
                    parts=[Part(root=TextPart(text=f"Forbidden: {e}"))]
                )
            )
        except Exception as e:
            log.exception("[a2a] analyst run failed")
            await updater.failed(
                updater.new_agent_message(
                    parts=[Part(root=TextPart(text=str(e)))]
                )
            )

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        task_id = context.task_id
        context_id = context.context_id
        if not task_id or not context_id:
            return
        updater = TaskUpdater(event_queue, task_id, context_id)
        await updater.cancel(
            message=updater.new_agent_message(
                parts=[Part(root=TextPart(text="Task canceled."))]
            )
        )
