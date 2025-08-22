from pathlib import Path
from typing import List, Tuple
from sqlalchemy.orm import Session

import csv
import boto3
import io

from .crud import bulk_insert
from .config import *



ORDERED_TABLES = ["departments", "jobs", "employees"]
CSV_SCHEMAS = {
    "departments": ["id", "department"],
    "jobs": ["id", "job"],
    "hired_employees": ["id", "name", "datetime", "department_id", "job_id"]
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


def read_csv_rows_from_s3(bucket: str, key: str):
    """
    Read csv file from bucket S3 and return list of dicts.
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )

    # Get object from S3
    obj = s3.get_object(Bucket=bucket, Key=str(key))
    body = obj["Body"].read().decode("utf-8")

    # Parse CSV
    reader = csv.DictReader(io.StringIO(body), fieldnames=CSV_SCHEMAS[key.stem])
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

            rows = read_csv_rows_from_s3(bucket="wcaraza", key=path)

            if not rows:
                results[original_name] = {"inserted": 0, "skipped": 0, "message": "empty file"}
                continue
            with db.begin():
                inserted = bulk_insert(db, table, rows)
            results[original_name] = {"inserted": inserted, "skipped": 0}
    return results