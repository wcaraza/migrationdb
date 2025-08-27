# migrationdb
Project to migrate local csv data to database on AWS RDS

## Tech Stack

- FastAPI for the REST API
- SQLAlchemy 2.0 (ORM + transactions)
- PostgreSQL 17 (AWS RDS)
- Docker


## Quickstart (SQLite, local)

1) Create and activate a virtualenv:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Run the API:

```bash
uvicorn app.main:app --reload
```

## Endpoints

```bash
curl -X POST "http://127.0.0.1:8000/ingest/csv/from-dir"
```

```bash
curl -G -d "year=2021" http://127.0.0.1:8000/metrics/employees-quarter-by-year
```

```bash
curl -G -d "year=2021" http://127.0.0.1:8000/metrics/hired-employees-by-department
```