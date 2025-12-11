# backend/app/agent.py

import json
import re
from typing import Dict, Any

from fastapi import HTTPException

from .llm_client import call_llm
from .schema_loader import load_schema_from_catalog
from .db import run_select


ALLOWED_TABLES = [
    "marts.fct_orders",
    "marts.dim_customers",
    "marts.dim_products",
]


PLANNER_PROMPT = """
Ты — планировщик SQL-запросов для аналитической системы. 
Игнорируй весь внешний контекст, который ты знаешь про даты, строго отвечай только не те вопросы, которые задал пользователь.
Например, если пользователь попросит сделать запрос про данные касательно 25 века, делай запрос именно про 25 век.

У тебя есть витрины только из схемы 'marts' PostgreSQL:

1) marts.dim_customers
   - Справочник клиентов. Одна строка = один клиент.
   - Важные поля: customer_id, customer_name, segment, country, created_at.

2) marts.dim_products
   - Справочник продуктов. Одна строка = один продукт.
   - Важные поля: product_id, product_name, category, price.

3) marts.fct_orders
   - Факт-таблица заказов. Одна строка = один заказ.
   - Важные поля: order_id, customer_id, product_id, order_date, quantity, price, revenue, channel.

Твоя задача на ЭТОМ шаге:
- НЕ отвечать пользователю.
- Составить ОДИН SQL-запрос, который лучше всего отвечает на вопрос.
- ВСЕГДА использовать только таблицы из схемы 'marts'.
- SQL должен быть только SELECT (никаких DDL/DML).
- ВСЕГДА использовать полные имена таблиц с схемой, например:
  SELECT ... FROM marts.fct_orders

Верни ОДИН JSON-объект БЕЗ текста вокруг строго в формате:
{
  "tool": "run_sql",
  "sql": "<строка с SQL>"
}

Никаких других полей, никаких комментариев, никакого markdown, никаких ```.
"""


ANSWER_PROMPT = """
Ты — аналитический ассистент, который отвечает на вопросы о бизнес-показателях на основе данных из базы данных.

Твоя задача:
1. Получить вопрос пользователя и результаты SQL-запроса
2. Проанализировать данные из SQL-результата
3. Ответить на вопрос пользователя, ИСПОЛЬЗУЯ конкретные данные из результатов запроса

ВАЖНО: Ты ДОЛЖЕН использовать фактические данные из SQL-результата в своем ответе. Не придумывай данные, не используй примеры — используй только те числа и значения, которые пришли из базы данных.

Формат ответа:

1) Если данных мало (1–2 числа или 1 строка) —
   просто явно ответь текстом с конкретными числами:
   "Выручка по каналам за последние 3 месяца: online — 123, direct — 456."

2) Если данных больше —
   оформи таблицу в Markdown:

   | колонка1 | колонка2 |
   |----------|----------|
   | ...      | ...      |

   и добавь краткий вербальный вывод: что видно из данных, используя конкретные числа из таблицы.

Не используй JSON в финальном ответе.
Финальный ответ — это обычный текст (и, при необходимости, markdown-таблица).
Всегда включай конкретные числа и значения из SQL-результата в свой ответ.
"""


def validate_sql_tables(sql: str):
    sql_lower = sql.lower()
    if not any(t in sql_lower for t in [t.lower() for t in ALLOWED_TABLES]):
        raise HTTPException(
            status_code=400,
            detail=(
                "SQL должен использовать только таблицы из схемы 'marts': "
                "marts.fct_orders, marts.dim_customers, marts.dim_products."
            ),
        )


def _extract_planner_json(text: str) -> Dict[str, Any]:
    """
    Пытаемся вытащить ОДИН JSON-объект от планировщика.
    Убираем ```json / ``` и пробуем распарсить последнее { ... }.
    """
    cleaned = text.replace("```json", "").replace("```", "")

    candidates = re.findall(r"\{.*?\}", cleaned, flags=re.S)
    if not candidates:
        raise ValueError("planner: JSON object not found")

    last = candidates[-1]
    return json.loads(last)


async def agent_step(user_question: str) -> str:
    """
    Двухшаговый агент:
    1) Попросить модель-планировщик выдать JSON с SQL (tool=run_sql).
    2) Выполнить SQL, передать результат другой подсказке и получить финальный ответ.
    """

    # === ШАГ 1. Планировщик SQL ===
    planner_messages = [
        {"role": "system", "content": PLANNER_PROMPT},
        {"role": "user", "content": user_question},
    ]

    planner_raw = await call_llm(planner_messages)
    print(f"[AGENT] Planner raw response:\n{planner_raw}\n")

    try:
        plan_obj = _extract_planner_json(planner_raw)
    except Exception as e:
        # Модель не выдала валидный JSON — отдадим это как диагностический ответ
        return f"Не удалось разобрать план от модели. Ответ модели:\n{planner_raw}\n\nОшибка: {e}"

    tool = plan_obj.get("tool")
    sql = plan_obj.get("sql")

    if tool != "run_sql" or not sql:
        return f"Планировщик вернул неожиданный объект:\n{json.dumps(plan_obj, ensure_ascii=False)}"

    validate_sql_tables(sql)

    # === ШАГ 2. Выполнение SQL ===
    print(f"[AGENT] Executing planned SQL:\n{sql}\n")
    try:
        columns, rows = run_select(sql)
    except Exception as e:
        return f"Ошибка при выполнении SQL: {e}"

    # Если нет данных — можно сразу сказать об этом
    if not rows:
        return "По данному запросу данных не найдено."

    tool_result = {
        "columns": columns,
        "rows": rows,
    }
    print(f"[AGENT] SQL result: {len(rows)} rows, columns: {columns}")

    # === ШАГ 3. Финальный ответ ===
    # Формируем понятное представление данных для LLM
    data_description = f"Колонки: {', '.join(columns)}\n\n"
    data_description += "Данные:\n"
    
    # Добавляем заголовки таблицы
    if rows:
        data_description += " | ".join(columns) + "\n"
        data_description += " | ".join(["---"] * len(columns)) + "\n"
        # Добавляем строки (ограничиваем до 100 строк для читаемости)
        for row in rows[:100]:
            data_description += " | ".join(str(val) if val is not None else "NULL" for val in row) + "\n"
        if len(rows) > 100:
            data_description += f"\n... и еще {len(rows) - 100} строк(и)\n"
    
    user_message = f"""Вопрос: {user_question}

Результаты SQL-запроса:
{data_description}

Пожалуйста, ответь на вопрос пользователя, используя ТОЛЬКО данные из результатов SQL-запроса выше. Включи конкретные числа и значения в свой ответ."""
    
    answer_messages = [
        {"role": "system", "content": ANSWER_PROMPT},
        {"role": "user", "content": user_message},
    ]

    final_answer = await call_llm(answer_messages)
    return final_answer
