# xbase_ai.py
# pip install python-dotenv langchain-openai langchain-core langchain-community faiss-cpu

import os, traceback
from dotenv import load_dotenv
import requests

from langchain_core.tools import tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables import RunnableParallel
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.messages import ToolMessage
from langchain_community.vectorstores import FAISS

from RunSQL import run_sql

# --------------------------------------------------
# Load environment
# --------------------------------------------------
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY missing")

# --------------------------------------------------
# Load RAG documents
# --------------------------------------------------
with open("SQL_documentation.txt", "r", encoding="utf-8") as f:
    SQL_RAG_TEXT = f.read()

with open("CSV_documentation.txt", "r", encoding="utf-8") as f:
    CSV_RAG_TEXT = f.read()

emb = OpenAIEmbeddings(openai_api_key=api_key)
chat_llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)

sql_vecstore = FAISS.from_texts([SQL_RAG_TEXT], emb)
csv_vecstore = FAISS.from_texts([CSV_RAG_TEXT], emb)

sql_retriever = sql_vecstore.as_retriever(search_kwargs={"k": 3})
csv_retriever = csv_vecstore.as_retriever(search_kwargs={"k": 3})

# --------------------------------------------------
# Tools
# --------------------------------------------------
@tool
def Run_Python(bucket_url: str, input: str, image_box: list[str] | None = None) -> dict:
    """Execute Python via external runner API."""
    try:
        resp = requests.post(
            "https://pythonbackend-xbase.onrender.com/run",
            json={"code": input, "bucket_url": bucket_url},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        images = data.get("images", [])
        if isinstance(image_box, list):
            image_box.extend(images)
        return data
    except Exception as e:
        return {"output": None, "error": str(e), "images": [], "bucket_url": bucket_url}


@tool
def Run_SQL(parent_id: str, input: str):
    """Run SQL query inside schema."""
    schema = "schema" + parent_id.replace("-", "_")
    try:
        run_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        run_sql(f"SET search_path TO {schema}")
        res = run_sql(input)
        if res is None:
            return "Query executed"
        return [list(r) for r in res]
    except Exception:
        return "SQL error:\n" + traceback.format_exc()


TOOLS = [Run_Python, Run_SQL]

# --------------------------------------------------
# Prompt
# --------------------------------------------------
prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are XBase AI.\n\n"
        "DATABASE INFO:\n{db_info}\n\n"

        "ABSOLUTE RULES:\n"
        "1) If db_info starts with 'CSV:', this is NOT a SQL database.\n"
        "   - NEVER generate SQL\n"
        "   - ONLY generate Python using pandas DataFrame `df`\n"
        "   - ALWAYS use Run_Python\n\n"

        "2) If db_info starts with 'SQL:', this IS a SQL database.\n"
        "   - Use SQL\n"
        "   - Use Run_SQL\n\n"

        "3) If the user asks about db_info, file info, schema info, or bucket_url:\n"
        "   - DO NOT use tools\n"
        "   - Explain db_info directly\n\n"

        "RAG CONTEXT (only if applicable):\n{context}\n\n"

        "EXECUTION RULES:\n"
        "- Always show the exact SQL or Python code BEFORE running\n"
        "- Ask: \"Do you want me to run this? (Accept/Decline)\"\n"
        "- Run tools ONLY after Accept\n"
        "- When using Python, assume `df` already exists\n"
    ),
    MessagesPlaceholder("chat_history"),
    ("human", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

# --------------------------------------------------
# Build agent with CONDITIONAL RAG
# --------------------------------------------------
def build_chat_agent():
    retrieval = RunnableParallel({
        "context": lambda x: (
            sql_retriever.invoke(x["input"])
            if x["db_info"].startswith("SQL")
            else csv_retriever.invoke(x["input"])
            if x["db_info"].startswith("CSV")
            else ""
        ),
        "input": lambda x: x["input"],
        "db_info": lambda x: x["db_info"],
        "chat_history": lambda x: x.get("chat_history", []),
        "agent_scratchpad": lambda x: x.get("agent_scratchpad", []),
    })

    return retrieval | prompt | chat_llm.bind_tools(TOOLS, tool_choice="auto")


CHAT_AGENT = build_chat_agent()

# --------------------------------------------------
# Tool executor
# --------------------------------------------------
def execute_tool(tool_obj, args):
    try:
        return tool_obj.invoke(args)
    except Exception:
        return "Tool execution error:\n" + traceback.format_exc()

# --------------------------------------------------
# MAIN ENTRY
# --------------------------------------------------
def Ask_AI(
    db_info: str,
    parent_id: str,
    query: str,
    chat_history=None,
    permission: bool = True,
    image_box: list[str] | None = None
):
    if chat_history is None:
        chat_history = []
    if image_box is None:
        image_box = []

    sql_res = []
    py_res = []

    response = CHAT_AGENT.invoke({
        "input": query,
        "db_info": db_info,
        "chat_history": chat_history,
        "agent_scratchpad": [],
    })

    tool_calls = getattr(response, "tool_calls", [])

    if not tool_calls or not permission:
        return response.content, chat_history, image_box, sql_res, py_res

    tool_messages = []
    results_text = ""

    for call in tool_calls:
        tool_name = call["name"]
        args = dict(call["args"])
        if tool_name == "Run_SQL":
            args["parent_id"] = parent_id
        if tool_name == "Run_Python":
            args.setdefault("image_box", image_box)

        tool = {t.name: t for t in TOOLS}[tool_name]
        out = execute_tool(tool, args)

        results_text += f"[{tool_name} OUTPUT]\n{out}\n\n"

        if tool_name == "Run_SQL":
            sql_res.append(out)
        if tool_name == "Run_Python":
            py_res.append(out)

        tool_messages.append(ToolMessage(
            content=str(out),
            tool_call_id=call["id"]
        ))

    final = CHAT_AGENT.invoke({
        "input": f"Summarize what happened:\n{results_text}",
        "db_info": db_info,
        "chat_history": chat_history,
        "agent_scratchpad": [response, *tool_messages],
    })

    return final.content, chat_history, image_box, sql_res, py_res

# --------------------------------------------------
# Interactive mode
# --------------------------------------------------
if __name__ == "__main__":
    print("XBase AI Chat Running (type 'stop' to exit)\n")

    db_info = (
        "CSV:\n"
        "File name: msdb.csv\n"
        "bucket_url:56103995-7437-499e-befd-eb6a6f12cb0e/1765220461721_msdb.csv"
    )

    chat_history = []

    while True:
        user_input = input("You: ")
        if user_input.lower() == "stop":
            break

        reply, chat_history, images, sql_res, py_res = Ask_AI(
            db_info=db_info,
            parent_id="56103995-7437-499e-befd-eb6a6f12cb0e",
            query=user_input,
            chat_history=chat_history,
            permission=True
        )

        print("\nAI:", reply)
