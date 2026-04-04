"""Quick CLI to ask the Analyst agent a question and see its full plan.

Usage
-----
    cd /Users/tomerfriedman/Development/gtmdb
    .venv/bin/python scripts/ask.py "What is the LTV of each campaign?"
"""

import asyncio
import sys

sys.path.insert(0, "src")

from dotenv import load_dotenv

load_dotenv(".env")

from gtmdb.analyst import AnalystRunner  # noqa: E402
from gtmdb.config import GtmdbSettings  # noqa: E402
from gtmdb.connect import connect_gtmdb  # noqa: E402


async def ask(question: str) -> None:
    settings = GtmdbSettings()
    db, _ = await connect_gtmdb(settings=settings, api_key=settings.admin_key)
    runner = AnalystRunner(db)

    print()
    print("=" * 70)
    print(f"QUESTION: {question}")
    print("=" * 70)

    async for chunk in runner.stream(question, verbose=True):
        print(chunk, end="", flush=True)

    print()
    await db.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: .venv/bin/python scripts/ask.py \"your question here\"")
        sys.exit(1)

    asyncio.run(ask(" ".join(sys.argv[1:])))
