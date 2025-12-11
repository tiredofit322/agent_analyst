# backend/app/schema_loader.py

import json
import os
from pathlib import Path
from typing import List

from .models import DatabaseSchema, TableSchema, ColumnSchema


def _get_catalog_path() -> Path:
    """
    Путь до dbt/target/catalog.json.
    Берём каталог на два уровня выше (из backend/app → backend → root),
    потом добавляем dbt/target/catalog.json.
    """
    app_dir = Path(__file__).resolve().parent  # backend/app
    root_dir = app_dir.parent.parent           # biz-analytics-mvp/
    catalog_path = root_dir / "dbt" / "target" / "catalog.json"
    if not catalog_path.exists():
        raise FileNotFoundError(f"catalog.json not found at {catalog_path}")
    return catalog_path


def load_schema_from_catalog(schema_filter: str = "marts") -> DatabaseSchema:
    """
    Читает dbt catalog.json и возвращает схемы (по умолчанию только schema='marts').
    """
    catalog_path = _get_catalog_path()
    with catalog_path.open("r", encoding="utf-8") as f:
        catalog = json.load(f)

    tables: List[TableSchema] = []

    # catalog["nodes"] — словарь моделей dbt
    for node_name, node in catalog.get("nodes", {}).items():
        meta = node.get("metadata", {})
        table_schema = meta.get("schema")
        table_name = meta.get("name")
        table_comment = meta.get("comment") or node.get("description")

        if schema_filter and table_schema != schema_filter:
            continue

        columns_meta = node.get("columns", {}) or {}
        columns: List[ColumnSchema] = []

        for col_name, col_info in columns_meta.items():
            col_type = col_info.get("type")
            col_comment = col_info.get("comment") or col_info.get("description")
            columns.append(
                ColumnSchema(
                    name=col_name,
                    type=col_type,
                    description=col_comment,
                )
            )

        tables.append(
            TableSchema(
                schema=table_schema,
                name=table_name,
                description=table_comment,
                columns=columns,
            )
        )

    return DatabaseSchema(tables=tables)
