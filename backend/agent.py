from langchain_ollama import ChatOllama
from deepagents import create_deep_agent
from db import get_tables_description, get_columns_description, run_select



llm = ChatOllama(
    model="qwen3:8b",
    temperature=0.2,
    top_p=0.9
)

research_instructions = """You are a database expert. Your job is to help the user with database queries.

## `get_tables_description`
Use this to get the description of the tables in the database.

## `get_columns_description`
Use this to get the description of the columns in a table. You need to provide the table_name parameter (just the table name, without schema prefix).

## `run_select`
Use this to run a SELECT query on the database. Feel free to use the tools to get the information you need to answer the user's question. You are only allowed to run SELECT queries, no other queries (drop, insert, update, delete...) are allowed.
Make sure to write the whole path to the table, including the schema name you found in the get_tables_description or get_columns_description tools. Accessing the tables directly by name is not allowed, only the full path is allowed.

You can use the tools to get the information you need to answer the user's question.
I assume your common flow would be:
1. Get the description of the tables in the database.
2. Based on the description, decide which tables are relevant to the user's question.
3. Get the description of the columns in the relevant tables.
4. Run a SELECT query on the relevant tables to get the information you need to answer the user's question.
5. Return the result to the user.

If the user's question is not related to the database, you should say so.
DO NOT MAKE UP INFORMATION. Answer only based on the information you accessed from the database.
"""

agent = create_deep_agent(
    model=llm,
    tools=[get_tables_description, get_columns_description, run_select],
    system_prompt=research_instructions
)

result = agent.invoke({"messages": [{"role": "user", "content": "What is the total number of the customers bought products in 2024 in each of the channels?"}]})

print(result["messages"][-1].content)
