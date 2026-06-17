# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment

Airflow 2.10.3 running via Docker Compose with LocalExecutor. Four containers: `airflow-webserver` (port 8080), `airflow-scheduler`, `postgres` (Airflow metadata DB), and `app-postgres` (application data, port 5433).

The Docker daemon must be running before any `docker compose` commands. On Arch Linux / WSL2:
```bash
sudo systemctl start docker
```

## Common commands

```bash
# Start the environment (first run builds the image)
docker compose up --build -d

# Restart after code changes to dags/
docker compose restart airflow-scheduler

# Trigger a DAG run with a historical date (current dates return no OpenFDA data)
docker compose exec airflow-scheduler airflow dags trigger openfda_adverse_events --exec-date 2024-03-15T10:00:00+00:00

# Check task states for a run
docker compose exec airflow-scheduler airflow tasks states-for-dag-run openfda_adverse_events <run_id>

# Inspect loaded data
docker compose exec app-postgres psql -U appuser -d pharma_db -c "SELECT * FROM adverse_events LIMIT 10;"

# Tail scheduler logs (useful for DAG parse errors)
docker compose logs -f airflow-scheduler
```

## Architecture

The DAG (`dags/openfda_pipeline.py`) defines the flow; business logic lives in `dags/utils/` and is imported directly since Airflow adds `dags/` to `sys.path`.

**Data flow:**
1. `extract.py` — calls OpenFDA `/drug/event.json` via `HttpHook` using `data_interval_start` as the query date (`receivedate:YYYYMMDD`), returns up to 10 raw records via XCom. Returns `[]` on HTTP 404 (no data for that date).
2. `transform.py` — pulls raw XCom, flattens nested `patient.drug[].medicinalproduct` and `patient.reaction[].reactionmeddrapt` into comma-separated strings.
3. `branch_on_data` (inline in DAG) — routes to `load_to_postgres` if transformed data is non-empty, otherwise `skip`.
4. `load.py` — creates `adverse_events` table if not exists, inserts rows via `PostgresHook` with `ON CONFLICT (report_id) DO NOTHING`.

**Connections** are injected via environment variables in `compose.yaml` (no UI setup needed):
- `openfda_api` — HTTP connection to `api.fda.gov`
- `app_postgres` — PostgreSQL connection to the `app-postgres` container

## Known gotchas

- Querying dates in 2025+ returns no results from OpenFDA (future data). Always use historical dates for manual triggers.
- The `logs/` directory must be owned by uid `50000` (Airflow's container user). If the scheduler crashes with `PermissionError` on `logs/dag_processor_manager`, run `sudo chown -R 50000:0 logs/`.
- The Docker socket at `/var/run/docker.sock` requires the `docker` group. If permission denied, run `sudo chmod 666 /var/run/docker.sock` for the current session.
