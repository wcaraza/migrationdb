from __future__ import annotations
from typing import List, Literal, get_args
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv

from . import models


load_dotenv()


#MAX_BATCH_SIZE = os.getenv("MAX_BATCH_SIZE")
MAX_BATCH_SIZE = 1000
BulkTable = Literal["departments", "jobs", "employees"]


def bulk_insert(
        db: Session, 
        table: BulkTable, 
        rows: List[dict]
        ) -> int:
    total_inserted=0

    tables_list = list(get_args(BulkTable))

    model_map = {
        "departments": models.Department,
        "jobs": models.Job,
        "employees": models.Employee
    }
    
    for tname in tables_list:
        
        model = model_map[tname]

        if table == tname:
            k=0

            for i in range(0, len(rows), MAX_BATCH_SIZE):
                k+=1
                chunk = rows[i : i + MAX_BATCH_SIZE]

                objs = [model(**row) for row in chunk]
                db.add_all(objs)

                print(f"processing table {table} - batch process: {k}")
                total_inserted += len(objs)

            return total_inserted

    raise ValueError(f"Unsupported table: {table}")


def employee_quarter(year: int, db: Session):
    query = text(f"""
        SELECT d.department,
               j.job,
               SUM(CASE WHEN EXTRACT(QUARTER FROM h.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
               SUM(CASE WHEN EXTRACT(QUARTER FROM h.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
               SUM(CASE WHEN EXTRACT(QUARTER FROM h.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
               SUM(CASE WHEN EXTRACT(QUARTER FROM h.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM employees h
        JOIN departments d ON h.department_id = d.id
        JOIN jobs j ON h.job_id = j.id
        WHERE EXTRACT(YEAR FROM h.datetime) = {year}
        GROUP BY d.department, j.job
        ORDER BY d.department, j.job
    """)
    result = db.execute(query).mappings().all()
    return result

def hired_employees_by_department(year: int, db: Session):
    query = text(f"""
        WITH hires AS (
            SELECT d.id, d.department, COUNT(*) AS hired
            FROM employees h
            JOIN departments d ON h.department_id = d.id
            WHERE EXTRACT(YEAR FROM h.datetime) = {year}
            GROUP BY d.id, d.department
        )
        SELECT id, department, hired
        FROM hires
        WHERE hired > (SELECT AVG(hired) FROM hires)
        ORDER BY hired DESC;
    """)
    result = db.execute(query).mappings().all()
    return result