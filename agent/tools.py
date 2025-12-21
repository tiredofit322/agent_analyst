import os
import psycopg2
import requests
from dotenv import load_dotenv
from contextlib import contextmanager
from decimal import Decimal
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()


DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


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
    """Run a SELECT query and return the columns and data
    
    Args:
        sql: The SQL query to run

    Returns:
        A tuple of the columns and data
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            logger.info(f"[DB] Executing SQL:\n{sql}\n")
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                logger.info("[DB] Result: 0 rows\n")
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

            logger.info(f"[DB] Result: {len(data)} rows\n")
            return columns, data

def get_tables_description() -> tuple[list[str], list[list]]:
    """Get the description of the tables in the database"""
    sql = """
        SELECT
            n.nspname        AS schema,
            c.relname        AS table_name,
            d.description    AS description
        FROM pg_class c
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        LEFT JOIN pg_description d
            ON d.objoid = c.oid
        AND d.objsubid = 0
        WHERE n.nspname = 'marts'
        AND c.relkind = 'v'
    """
    columns, data = run_select(sql)
    return columns, data


def get_columns_description(table_name: str) -> tuple[list[str], list[list]]:
    """Get the description of the columns in the table
    
    Args:
        table_name: The name of the table to get the description of the columns for. Just the table name, without schema prefix.

    Returns:
        A tuple of the columns and data
    """
    sql = f"""
        SELECT
            n.nspname        AS schema,
            c.relname        AS table_name,
            a.attname        AS column_name,
            d.description    AS description
        FROM pg_class c
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        JOIN pg_attribute a
            ON a.attrelid = c.oid
        LEFT JOIN pg_description d
            ON d.objoid = c.oid
        AND d.objsubid = a.attnum
        WHERE n.nspname = 'marts'
        AND c.relkind = 'v'  -- tables, views, mat views
        AND a.attnum > 0                  -- exclude system columns
        AND c.relname = '{table_name}'
        AND NOT a.attisdropped
    """
    columns, data = run_select(sql)
    return columns, data

def python_exec(code: str) -> str:
    """Execute Python code and return the result"""

    BASE_URL = "http://localhost:8000"

    def python_exec(code: str) -> str:
        # Create a new session
        try:
            logger.info(f"[PYTHON EXEC] Creating session")
            resp = requests.post(f"{BASE_URL}/sessions")
            resp.raise_for_status()
            session_id = resp.json()["session_id"]
        except Exception as e:
            return f"Failed to create session: {e}"

        # Execute code through /exec endpoint
        try:
            exec_payload = {
                "session_id": session_id,
                "code": code,
                "timeout_sec": 10
            }
            logger.info(f"[PYTHON EXEC] Executing code: {code}")
            exec_resp = requests.post(f"{BASE_URL}/exec", json=exec_payload)
            exec_resp.raise_for_status()
            result = exec_resp.json()
        except Exception as e:
            # Clean up session even if code execution fails
            try:
                requests.delete(f"{BASE_URL}/sessions/{session_id}")
            except Exception:
                pass
            return f"Error during code execution: {e}"

        # Delete the session afterwards
        try:
            requests.delete(f"{BASE_URL}/sessions/{session_id}")
        except Exception:
            pass

        if not result.get("ok", False):
            error_msg = result.get("error", "Unknown error")
            outputs = result.get("outputs", [])
            return f"Execution failed: {error_msg}\n{outputs}"

        outputs = result.get("outputs", [])
        output_text = []

        for out in outputs:
            if out.get("type") == "stream":
                output_text.append(out.get("text", ""))
            elif out.get("type") in ("execute_result", "display_data"):
                data = out.get("data", {})
                if data.get("text/plain"):
                    output_text.append(data["text/plain"])
            elif out.get("type") == "error":
                output_text.append(
                    f"Error: {out.get('ename', '')}: {out.get('evalue', '')}\n"
                    + "\n".join(out.get("traceback", []))
                )

        return "\n".join(output_text).strip() if output_text else "No output."

    return "Code executed successfully"