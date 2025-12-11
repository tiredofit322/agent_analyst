# backend/app/main.py

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from .agent import agent_step
import asyncio

from .models import DatabaseSchema, RunSQLRequest, RunSQLResponse
from .schema_loader import load_schema_from_catalog
from .db import run_select

class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    answer: str


app = FastAPI(
    title="LLM SQL Backend",
    description="Слой над Postgres для LLM-агента: /schema и /run_sql",
    version="0.1.0",
)

# Если будешь вызывать из браузера / другого клиента — CORS на всякий:
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # в проде лучше ограничить
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/schema", response_model=DatabaseSchema)
def get_schema():
    """
    Вернуть описание витрин (по умолчанию schema='marts') из dbt catalog.json.
    Это то, что будет видеть LLM как знание о данных.
    """
    try:
        db_schema = load_schema_from_catalog(schema_filter="marts")
        return db_schema
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))


def _validate_sql(sql: str):
    """
    Очень простая валидация:
    - только SELECT
    - запрещаем DDL/DML-ключевые слова
    - требуем упоминание схемы marts (чтобы не лез в raw/staging)
    """
    sql_stripped = sql.strip().lower()
    if not sql_stripped.startswith("select"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed.")

    forbidden_keywords = [
        " insert ", " update ", " delete ", " drop ", " alter ",
        " truncate ", " create ", " grant ", " revoke ",
    ]
    for kw in forbidden_keywords:
        if kw in sql_stripped:
            raise HTTPException(
                status_code=400,
                detail=f"Keyword '{kw.strip()}' is not allowed in SQL.",
            )

    # грубая проверка, что есть хоть одно обращение к marts.*
    if "marts." not in sql_stripped:
        raise HTTPException(
            status_code=400,
            detail="Query must reference schema 'marts'.",
        )

    # опционально ограничим длину
    if len(sql) > 5000:
        raise HTTPException(status_code=400, detail="SQL query is too long.")


@app.post("/run_sql", response_model=RunSQLResponse)
def run_sql(req: RunSQLRequest):
    """
    Выполнить безопасный SELECT к Postgres.
    Используется LLM-агентом как инструмент для получения данных.
    """
    _validate_sql(req.sql)

    try:
        columns, rows = run_select(req.sql)
    except Exception as e:
        # В проде лучше логировать и не отдавать подробности,
        # но для MVP можно вернуть текст ошибки.
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return RunSQLResponse(columns=columns, rows=rows)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/ask", response_model=AskResponse)
async def ask_agent(req: AskRequest):
    """
    Главный эндпоинт для пользователя или фронта:
    принимает вопрос на естественном языке, запускает агента, возвращает ответ.
    """
    answer = await agent_step(req.question)
    return AskResponse(answer=answer)