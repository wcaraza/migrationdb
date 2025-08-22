from __future__ import annotations
from pathlib import Path

from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session

from .config import *
from .database import SessionLocal
from .etl import collect_dir_csvs, ingest_files_in_order
from .crud import employee_quarter, hired_employees_by_department


app = FastAPI(title="DB Migration REST API", version="1.3.0")



def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/migration/csv-from-dir")
def migration_from_dir(
    db: Session = Depends(get_db)):
    dir_path = Path(DIR_ENV)
    files = collect_dir_csvs(dir_path)
    if not files:
        raise HTTPException(status_code=400, detail=f"No CSV files found in {dir_path} matching departments/jobs/employees.")
    print(files)
    results = ingest_files_in_order(db, files)
    return results

@app.get("/metrics/employees-quarter-by-year")
def metrics_employee_quarter_by_year(
        year: int, 
        db: Session = Depends(get_db)):
    return employee_quarter(year,db)

@app.get("/metrics/hired-employees-by-department")
def metrics_hired_employees_by_department(
        year: int, 
        db: Session = Depends(get_db)):
    return hired_employees_by_department(year,db)