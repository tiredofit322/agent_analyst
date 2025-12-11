import os
from contextlib import contextmanager
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


# грузим .env из корня проекта (на уровень выше backend/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(ENV_PATH)


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "bizmvp")
DB_USER = os.getenv("DB_USER", "analytics")
DB_PASSWORD = os.getenv("DB_PASSWORD", "analytics")


@contextmanager
def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    try:
        yield conn
    finally:
        conn.close()


def run_select(sql: str) -> tuple[list[str], list[list]]:
    """Выполнить SELECT и вернуть (имена_колонок, строки) с примитивными типами."""
    print(f"[DB] Executing SQL:\n{sql}\n")
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                print("[DB] Result: 0 rows\n")
                return [], []

            columns = list(rows[0].keys())
            data: list[list] = []

            for row in rows:
                row_values = []
                for col in columns:
                    value = row[col]
                    # Приводим Decimal к float (или можно к str, если так комфортнее)
                    if isinstance(value, Decimal):
                        value = float(value)
                    row_values.append(value)
                data.append(row_values)

            print(f"[DB] Result: {len(data)} rows\n")
            return columns, data


