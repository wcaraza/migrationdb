from __future__ import annotations
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from dotenv import load_dotenv

load_dotenv()


DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#print(DATABASE_URL)
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)


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