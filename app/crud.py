from __future__ import annotations
from typing import Iterable, List, Literal, get_args
from sqlalchemy.orm import Session

from . import models

MAX_BATCH_SIZE = 1000
BulkTable = Literal["departments", "jobs"]#employees


def bulk_insert(
        db: Session, 
        table: BulkTable, 
        rows: List[dict]
        ) -> int:
    total_inserted=0

    tables_list = list(get_args(BulkTable))

    model_map = {
        "departments": models.Department,
        "jobs": models.Job
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
                print(f"batch in table {table} - {k}")
                total_inserted += len(objs)

            return total_inserted

    raise ValueError(f"Unsupported table: {table}")