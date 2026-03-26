# GtmDB

<p align="center">
  <img src="docs/assets/gtm-db-logo.png" alt="GtmDB" width="280" />
</p>

## Overview — what is GtmDB?

**GtmDB** is the **full-stack data layer** built for the era of GTM autonomous operations. While most agentic systems rely on scattered data and rigid tables without awareness of permissions, GtmDB — backed by **multi-model storage** — provides agents exactly what they need to make high-integrity, autonomous choices without exposing unnecessary information. Whether your agents are built on **OpenAI Frontier, Claude Cowork, or LangGraph**, **GtmDB** provides them with secure access to your business data, so they can run your business.

### What GtmDB supports

1. **One system of record for GTM — and the agents that use it.** Typed CRUD and traversals over the commercial graph — accounts through campaigns plus activities, comms, events, insights, and operational stats in one entity model — so one store can back **MCP** servers, LangChain-style tools, and bindings for common agent stacks (**LangGraph**, **Vertex AI**, **OpenClaw**, and similar) instead of every assistant re-wrapping a different partial API per system.

2. **Security, permissions, and access out of the box — for people and for agents.** Every call runs under a `Scope` tied to declarative policies: tenant isolation, read/write rules, field-level masking, and redaction. Issue different tokens for a human rep, an internal agent, or an external partner so you **control exactly what each principal sees and can change**.

3. **Triggers and automation hooks.** Design your stack so events in GtmDB drive downstream behavior: e.g. **trigger an agent when a deal closes**, when a **campaign is sent**, or when signals indicate **learning** or coaching moments — closing the loop between CRM state and autonomous workflows.

4. **Agents can ask anything — from facts, to analytical and statistical questions.** The graph + indexed analytical store model supports exploratory and quantitative questions over your pipeline and GTM motion: strategic prompts (“*why are we losing to competitors?*” in context of your deals and accounts) and statistical views (funnels, attribution, stage distributions) on top of the same CRM-native store — not a separate BI silo.

## Sales, marketing & customer success — one system of record

The entity model is deliberately **cross-functional**: the same graph holds what **marketing**, **sales**, and **customer success (CS)** care about, so GtmDB can serve as the **ideal system of record for the whole revenue business** — not three disconnected spreadsheets or siloed tools.

- **Marketing** — `Campaign`, `Lead`, segments and lists, content and landing experiences, form and page-view events, attribution touchpoints, and links to `Deal` (`INFLUENCED`). Full-text search over accounts and programs; pipeline visibility stays tied to the same graph sales uses.

- **Sales** — `Account` / `Contact` hierarchy, `Deal`, quotes, orders, products, contracts, comms (email, call, meeting), tasks, and partner accounts — plus traversals (360°, path-finding) for account planning and multi-threading.

- **Customer success & support** — Tickets, SLAs, health scores, renewal context on the same `Account` graph as sales; timeline from emails, meetings, usage events, and notes so CS, sales, and marketing share one customer truth.

- **RevOps & leadership** — Metric snapshots, forecasts, dashboards/reports, and AI or rule-driven **insights** materialized on top of the same store — no shadow warehouse required for operational GTM questions.

Because every department reads and writes through the **same CRM-native API** under `Scope`, you avoid duplicate “shadow” CRMs: one store, one permission model, one place agents and humans query when they need the full picture of a customer or campaign.

Under the hood, GtmDB stores CRM entities as **nodes and relationships** in **Neo4j** (labels such as `Lead`, `Account`, `Deal`, etc.). That choice enables graph traversals (360° views, timelines, attribution paths, search). Applications integrate through the **`GtmDB`** Python client; host apps may persist issued tokens in their own database (e.g. via SQLAlchemy); the token row holds JSON policies that drive authorization.
