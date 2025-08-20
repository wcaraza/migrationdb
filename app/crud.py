from __future__ import annotations
from typing import Iterable, List, Literal, get_args
from sqlalchemy.orm import Session
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