# AGENTS.md — Project 1: Real-Time + Batch Lakehouse (Wikimedia Edits)

Put this file at the repo root as `AGENTS.md` so Codex reads it automatically. Work one milestone at a time. Do not start a milestone until the previous one's acceptance criteria pass.

## 1. Goal

Build a production-style data pipeline that ingests the live Wikimedia `recentchange` event stream, processes it with Spark Structured Streaming into a Delta Lake medallion architecture (bronze, silver, gold), models analytics marts with dbt, orchestrates with Airflow, enforces data quality, and ships with CI/CD and infrastructure as code.

This is a portfolio project for entry-level data engineer roles. Optimize for: correctness, reproducibility (`make up` runs everything locally), measurable results, and clear documentation. Every design choice must be explainable in an interview.

## 2. Source

- Wikimedia EventStreams (Server-Sent Events, free, no auth): `https://stream.wikimedia.org/v2/stream/recentchange`
- Each event is JSON with fields such as: `id`, `type` (edit/new/log/categorize), `title`, `user`, `bot`, `wiki`, `server_name`, `namespace`, `comment`, `timestamp` (epoch seconds), `length` {old,new}, `revision` {old,new}, `minor`, `patrolled`, `meta` {id, dt, domain, topic, partition, offset}.
- Use a descriptive `User-Agent` header when connecting (Wikimedia policy).
- Verify the real schema by sampling live events before writing models. Do not assume fields exist.

## 3. Architecture

```
Wikimedia SSE -> Producer (Python) -> Redpanda topic `wiki.recentchange`
   -> Spark Structured Streaming -> Delta BRONZE (raw, append-only)
   -> Spark job -> Delta SILVER (parsed, typed, deduped, validated)
   -> dbt (dbt-duckdb reading silver Delta) -> GOLD marts
   -> Streamlit dashboard
Airflow orchestrates: silver compaction/maintenance, dbt build, quality checks, metrics collection.
```

Decisions already made (do not change without asking):
- Local-first: everything runs in Docker Compose. Cloud deployment is the last milestone.
- Message broker: Redpanda (Kafka API compatible, single container).
- Table format: Delta Lake via `delta-spark`.
- Gold layer: dbt with `dbt-duckdb` reading silver Delta tables (fast in CI, no cluster needed).
- Orchestration: Airflow 2.x in Docker.
- Data quality: Great Expectations on silver, dbt tests on gold.
- Language: Python 3.11, SQL. Dependency management with `uv` or `pip-tools`, pinned versions.

## 4. Repo structure

```
wiki-lakehouse/
├── AGENTS.md
├── README.md                  # headline metrics first, then architecture, how to run
├── Makefile                   # up, down, test, lint, dbt-build, benchmark
├── docker-compose.yml         # redpanda, spark, airflow, postgres (airflow meta), streamlit
├── pyproject.toml
├── .github/workflows/ci.yml
├── docs/
│   ├── architecture.md        # diagram + decisions and tradeoffs
│   ├── data-dictionary.md
│   ├── runbook.md             # how to restart, backfill, recover
│   └── benchmarks.md          # measured numbers with methodology
├── src/
│   ├── producer/
│   │   └── sse_to_kafka.py    # reads SSE, publishes JSON to Redpanda, reconnect + backoff
│   ├── streaming/
│   │   ├── bronze_stream.py   # Kafka -> Delta bronze (raw payload + kafka metadata)
│   │   └── silver_stream.py   # bronze -> silver (parse, type, dedupe, watermark)
│   ├── quality/
│   │   └── expectations/      # Great Expectations suites for silver
│   ├── maintenance/
│   │   └── optimize_tables.py # compaction / OPTIMIZE / VACUUM
│   └── metrics/
│       └── collect.py         # writes run metrics to a metrics table/file
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml           # dbt-duckdb
│   ├── models/
│   │   ├── staging/           # stg_recentchange
│   │   └── marts/             # see section 6
│   └── tests/
├── airflow/
│   └── dags/
│       ├── maintenance_dag.py
│       └── dbt_quality_dag.py
├── dashboard/
│   └── app.py                 # Streamlit
├── infra/
│   └── terraform/             # cloud stage: storage account, Databricks workspace config
├── tests/
│   ├── unit/                  # parsing, dedupe logic, schema
│   └── integration/           # small end-to-end run on a recorded sample
└── data/
    └── samples/               # recorded events for tests (a few thousand lines)
```

## 5. Data layers

**Bronze** (`delta/bronze/recentchange`)
- Append-only. Columns: `raw_json` (string), `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingested_at`.
- No parsing, no filtering. Checkpoint location configured so restarts resume exactly.

**Silver** (`delta/silver/recentchange`)
- Parse JSON with an explicit schema (no inference). Cast types. Convert `timestamp` to `event_time` (timestamp type).
- Dedupe on `meta.id` using `dropDuplicatesWithinWatermark` or equivalent, with a 10-minute watermark.
- Partition by `event_date` (derived from `event_time`) and `wiki` only if cardinality is sane; justify the choice in docs.
- Invalid or unparseable records go to a quarantine table `delta/silver/_quarantine` with the error reason. Never drop silently.
- Handle schema evolution: unknown new fields must not break the job.

**Gold** (dbt marts, see next section)

## 6. Gold marts (dbt)

- `fct_edits_hourly`: edits per wiki per hour, bot vs human, new pages vs edits, avg change size.
- `dim_wiki`: wiki, server, language, project type.
- `mart_top_pages`: most edited pages per day per wiki.
- `mart_bot_activity`: bot share over time, top bots.
- `mart_revert_candidates` (stretch): pages with rapid successive edits by different users.

Every model needs: description, column docs, `not_null`/`unique` tests on keys, `accepted_values` where relevant, and at least one custom test (for example, edit counts must not be negative).

## 7. Data quality

- Great Expectations suite on silver: required columns non-null, `event_time` within a sane range, `wiki` and `type` from known values (log unknown values, do not fail on them), row count volume checks.
- Freshness check: fail the DAG if the newest silver `event_time` is older than 10 minutes during a streaming run.
- Quarantine rate metric: percent of events quarantined; alert threshold 1%.

## 8. Metrics to collect from day one

Write to `docs/benchmarks.md` (generated by `make benchmark`), and log each run to a metrics file or table:
- Sustained events/second, and peak tested.
- End-to-end latency p50/p95 (event `meta.dt` to silver commit time).
- Duplicate events detected, late events handled, quarantine rate.
- Recovery test: kill the streaming job, restart, record time to catch up; verify zero loss and zero duplicates.
- Query time on a gold query before and after compaction/partitioning.
- Cloud cost per month (cloud milestone).
Always record the test conditions (machine, data volume, duration).

## 9. Engineering standards

- Python: type hints, `ruff` for lint and format, `pytest` for tests. No hardcoded secrets; config via environment variables with a `.env.example`.
- Idempotent jobs: rerunning must not duplicate data.
- Small, focused commits with clear messages. One feature per commit where possible.
- Every module gets a docstring explaining its purpose. Comments explain why, not what.
- Unit tests for parsing, dedupe logic, and quality rules using recorded samples in `data/samples/`.
- CI (`.github/workflows/ci.yml`): ruff, pytest, `dbt build` against the sample data with duckdb, docker compose config validation.

## 10. Milestones and acceptance criteria

**M1: Ingestion (producer + Redpanda).**
Accept when: `make up` starts Redpanda and the producer; events appear in topic `wiki.recentchange`; producer reconnects after a dropped connection; a sample of 5,000 events is saved in `data/samples/`.

**M2: Bronze stream.**
Accept when: Spark job writes bronze Delta; restart resumes from checkpoint without duplicates in bronze row counts versus Kafka offsets.

**M3: Silver stream.**
Accept when: parsed typed silver table exists; duplicates removed; bad records land in quarantine; unit tests pass on samples.

**M4: Gold with dbt.**
Accept when: `dbt build` passes (models and tests) against silver; marts return sensible results; docs generated with `dbt docs generate`.

**M5: Orchestration and quality.**
Accept when: Airflow DAGs run maintenance and dbt build on schedule; Great Expectations suite runs and fails on injected bad data; freshness check works.

**M6: CI/CD.**
Accept when: GitHub Actions passes on every push; badge in README.

**M7: Dashboard and benchmarks.**
Accept when: Streamlit shows live edit rate, bot share, top pages; `make benchmark` produces `docs/benchmarks.md` with the metrics in section 8.

**M8: Cloud deployment (Azure + Databricks).**
Accept when: Terraform provisions storage and Databricks config; bronze and silver jobs run as Databricks jobs; costs measured; resources can be torn down with one command. Budget alerts configured before deploying.

**M9: Documentation.**
Accept when: README opens with 3-4 headline metrics, an architecture diagram, one-command run instructions, a "what broke and how I fixed it" section, and a link to the write-up.

## 11. How Codex should work in this repo

- Before coding a milestone, restate the plan in a few bullets and list the files you will create or change.
- Verify assumptions by running code (sample live events, check Spark/Delta versions) instead of guessing.
- After each milestone, run the tests and show the acceptance check results.
- If a dependency or version conflict appears, pin versions and note the decision in `docs/architecture.md`.
- Never add tools or services outside this spec without asking.
- Keep explanations of non-obvious choices in `docs/architecture.md` so the author can explain them in interviews.

## 12. Out of scope

Kubernetes, Iceberg migration, ML models, multi-region setup, user authentication, and anything not listed in the milestones.
