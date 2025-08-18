from __future__ import annotations
import os
from pathlib import Path
from typing import List, Literal

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse



app = FastAPI(title="DB Migration REST API", version="1.0.0")




MAX_BATCH_SIZE = 1000
ORDERED_TABLES = ["departments", "jobs", "hired_employees"]



def infer_table_from_filename(filename: str) -> str | None:
    lower = filename.lower()
    for t in ORDERED_TABLES:
        if t in lower:
            return t
    return None

def collect_dir_csvs(dir_path: Path) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    if not dir_path.exists() or not dir_path.is_dir():
        return out
    for p in dir_path.iterdir():
        if p.suffix.lower() == ".csv" and infer_table_from_filename(p.name):
            out.append((p.name, p))
    return out

@app.post("/ingest/csv/from-dir")
def ingest_from_dir():
    dir_env = os.getenv("INGEST_DIR", "./source_data")
    dir_path = Path(dir_env)
    files = collect_dir_csvs(dir_path)
    if not files:
        raise HTTPException(status_code=400, detail=f"No CSV files found in {dir_path} matching departments/jobs/employees.")
    print(files)
    return True
