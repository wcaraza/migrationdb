from __future__ import annotations
from datetime import date

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, DateTime, ForeignKey


class Base(DeclarativeBase):
    pass


class Department(Base):
    __tablename__ = "departments"
    department_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)


class Job(Base):
    __tablename__ = "jobs"
    job_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)


class Employee(Base):
    __tablename__ = "employees"
    employee_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    hire_date: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    department_id: Mapped[int] = mapped_column(ForeignKey("departments.department_id"), nullable=False)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.job_id"), nullable=False)
