from pathlib import Path
from typing import List, Dict, Tuple
from sqlalchemy.orm import Session

import csv

from .crud import bulk_insert



ORDERED_TABLES = ["departments", "jobs", "employees"]
CSV_SCHEMAS = {
    "departments": ["department_id", "name"],
    "jobs": ["job_id", "job_name"],
    "hired_employees": ["employee_id", "name", "hire_date", "department_id", "job_id"]
}

def infer_table_from_filename(
        filename: str) -> str | None:
    lower = filename.lower()
    for t in ORDERED_TABLES:
        if t in lower:
            return t
    return None

def collect_dir_csvs(
        dir_path: Path) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    if not dir_path.exists() or not dir_path.is_dir():
        return out
    for p in dir_path.iterdir():
        if p.suffix.lower() == ".csv" and infer_table_from_filename(p.name):
            out.append((p.name, p))
    return out

def read_csv_rows(
        path: Path) -> List[Dict]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f,fieldnames=CSV_SCHEMAS[path.stem])
        rows = list(reader)
    
    normalized = [{k: (v if v != "" else None) for k, v in row.items()} for row in rows]
    return normalized

def ingest_files_in_order(
        db: Session, 
        files: List[Tuple[str, Path]]
        ) -> dict:
    buckets: dict[str, List[Tuple[str, Path]]] = {t: [] for t in ORDERED_TABLES}
    for original_name, path in files:
        table = infer_table_from_filename(original_name)
        if not table:
            continue
        buckets[table].append((original_name, path))

    results = {}
    for table in ORDERED_TABLES:
        for original_name, path in buckets[table]:
            rows = read_csv_rows(path)
            if not rows:
                results[original_name] = {"inserted": 0, "skipped": 0, "message": "empty file"}
                continue
            with db.begin():
                inserted = bulk_insert(db, table, rows)
            results[original_name] = {"inserted": inserted, "skipped": 0}
    return results