# Production Readiness Checklist

Organized by phase. Each phase should be done in order before moving to the next.

---

## Phase 1 — Security & Secrets
> Do this before sharing anything

- [ ] **Rotate all API keys** (OpenAI, Anthropic) — assume they've been seen locally, rotate as a precaution
- [ ] **Create `.env.example`** with placeholder values for every variable actually used in `settings.py` — currently incomplete
- [ ] **Verify gitignore** — `reports/` and all `item-analysis/*.csv / *.xlsx` are ignored; confirm nothing slipped through
- [ ] **Audit git history** for any accidentally committed secrets (`git log -S "sk-"`)
- [ ] **Add `TODO.md` and `.DS_Store` to `.gitignore`** — currently untracked and leaking internal notes

---

## Phase 2 — Deployment Infrastructure

- [ ] **Dockerfile** — containerize the app (separate images for dashboard and job runner)
- [ ] **`docker-compose.yml`** — wire up Postgres + dashboard + job runner for local/staging
- [ ] **Pick a hosting target** — Streamlit Community Cloud (easiest), Railway, Render, or VPS (most control). Decision gates everything below.
- [ ] **Set up GitHub Actions CI** — run `pytest` on every push; block merge on failure
- [ ] **Database hosting** — Supabase, Railway Postgres, or managed RDS; get a `DATABASE_URL` for production
- [ ] **Run `alembic upgrade head`** against prod DB — verify all 4 migrations apply cleanly
- [ ] **Environment variable management** — use the host's secrets manager (Railway/Render have built-in secret stores; avoid committing `.env`)

---

## Phase 3 — Authentication
> Required before client access

- [ ] **Decide on auth model:**
  - *Single client, simple*: HTTP Basic Auth or a shared password via `st.secrets` + a login page
  - *Multi-client*: OAuth2 (Google/GitHub) via `streamlit-authenticator` or a reverse proxy (Nginx + OAuth2 Proxy)
- [ ] **Add login gate to `dashboard/app.py`** — no page should be reachable without auth
- [ ] **Client isolation** — if serving multiple clients, add `tenant_id` to `db/models.py` and filter all queries by it; otherwise one client can see another's data
- [ ] **Add `User` / `Tenant` table in Alembic migration** if going multi-tenant

---

## Phase 4 — Scheduling & Job Observability

- [ ] **Replace manual cron** with APScheduler or a hosted cron (Railway Cron, GitHub Actions scheduled workflow) — manual cron fails silently
- [ ] **Add job run log table** (`job_runs`: job_name, started_at, finished_at, status, error) — currently no record of when scrapes last ran or failed
- [ ] **Expose last-run status in the dashboard** — clients need to know if data is fresh
- [ ] **Add email/Slack alert on job failure** — right now failures are silent outside the terminal

---

## Phase 5 — Reliability & Error Handling

- [ ] **Add Sentry** for exception tracking — `pip install sentry-sdk[fastapi,sqlalchemy]`, one `sentry_sdk.init()` call covers all jobs and dashboard
- [ ] **Switch to `structlog`** — already in `pyproject.toml` but unused; structured logs are searchable in any log aggregator
- [ ] **Add retry + backoff to LLM calls** — currently a 429 silently drops a listing (`llm_matcher.py:260-262`)
- [ ] **Add DB connection pooling config** — set `pool_size`, `max_overflow`, `pool_pre_ping=True` for prod
- [ ] **Allegro scraper bot-detection hardening** — blocks after ~100 items (documented in `TODO.md`); fix before going live or data will be stale

---

## Phase 6 — Dashboard Polish for Clients

- [ ] **Replace internal jargon** — competitor IDs like `fermatshop_sk`, `strendpro` etc. should use display names
- [ ] **Add data freshness indicator** — show "last scraped: 2 hours ago" on every page
- [ ] **Add CSV/Excel export button** per dashboard view — clients will want to download
- [ ] **Mobile/tablet check** — Streamlit is mostly fine but test on a tablet
- [ ] **Custom domain** — set up a subdomain like `pricing.yourcompany.com`
- [ ] **Logo / branding** — add company logo to the Streamlit sidebar (`st.logo()`)

---

## Phase 7 — Docs & Handoff

- [ ] **Client-facing README** — separate from the dev README; explain what the dashboard shows and how to read it
- [ ] **SLA definition** — how often does data refresh? What's the uptime commitment?
- [ ] **Runbook** — what to do when Allegro scraper breaks, when LLM costs spike, when DB is full
- [ ] **`.env.example` complete** — every prod environment variable documented with a description

---

## Quick Wins (do today)

| Task | Effort | Impact |
|------|--------|--------|
| Add `TODO.md` + `.DS_Store` to `.gitignore` | 5 min | Stops leaking internal notes |
| Set up Sentry | 15 min | Immediate error visibility across whole app |
| GitHub Actions CI (`pytest` on push) | 30 min | Prevents broken deployments |
| `streamlit-authenticator` login page | 1–2 hrs | Unblocks client sharing |

> **Biggest blocker before sharing with clients: Phase 3 (auth).** Without it, anyone with the URL sees all data.
