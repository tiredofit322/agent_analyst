# backend/app/models.py

from typing import List, Any
from pydantic import BaseModel


class ColumnSchema(BaseModel):
    name: str
    type: str | None = None
    description: str | None = None


class TableSchema(BaseModel):
    schema: str
    name: str
    description: str | None = None
    columns: List[ColumnSchema]


class DatabaseSchema(BaseModel):
    tables: List[TableSchema]


class RunSQLRequest(BaseModel):
    sql: str


class RunSQLResponse(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
