# xbase_ai.py
# pip install python-dotenv langchain-openai langchain-core langchain-community faiss-cpu

import os, io, sys, traceback
from dotenv import load_dotenv

from langchain_core.tools import tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables import (
    RunnablePassthrough,
    RunnableParallel,
    RunnableSerializable,
)
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.messages import ToolMessage, HumanMessage, AIMessage
import requests

from langchain_community.vectorstores import FAISS
from RunSQL import run_sql

# -----------------------------
# Load environment + RAG text
# -----------------------------
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY missing")

with open("SQL_documentation.txt", "r", encoding="utf-8") as f:
    RAG_TEXT = f.read()

emb = OpenAIEmbeddings(openai_api_key=api_key)
chat_llm = ChatOpenAI(openai_api_key=api_key, model="gpt-3.5-turbo", temperature=0)

# build vector store
vecstore = FAISS.from_texts([RAG_TEXT], emb)
retriever = vecstore.as_retriever(search_kwargs={"k": 3})


# -----------------------------
# Tools
# -----------------------------
@tool
def Run_Python(bucket_url: str, input: str, image_box: list[str] | None = None) -> dict:
    """Execute Python via external runner API and return structured result.

    Calls https://pythonbackend-xbase.onrender.com with JSON body
    {"code": input, "bucket_url": bucket_url} and returns a dict with
    keys: output, error, images, bucket_url.
    """
    try:
        resp = requests.post(
            "https://pythonbackend-xbase.onrender.com",
            json={"code": input, "bucket_url": bucket_url},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        images = data.get("images", [])
        # Optionally accumulate images into provided image_box
        if isinstance(image_box, list) and images:
            image_box.extend(images)
        return {
            "output": data.get("output"),
            "error": data.get("error"),
            "images": images,
            "bucket_url": data.get("bucket_url", bucket_url),
        }
    except requests.RequestException as e:
        return {
            "output": None,
            "error": f"Run_Python request failed: {e}",
            "images": [],
            "bucket_url": bucket_url,
        }
    except ValueError:
        return {
            "output": None,
            "error": "Run_Python: invalid JSON response",
            "images": [],
            "bucket_url": bucket_url,
        }


@tool
def Run_SQL(parent_id: str, input: str) -> str:
    """Runs SQL query using CRUD.run_sql."""
    parent_id = "schema" + parent_id.replace('-', '_')
    try:
        res = run_sql("SELECT current_schema();")
        current_schema = res[0][0] if res else None
    except Exception:
        current_schema = None

    # --- STEP 2: switch schema if different ---
    if current_schema != parent_id:

        # ensure schema exists
        run_sql(f"CREATE SCHEMA IF NOT EXISTS {parent_id}")

        # switch to it
        run_sql(f"SET search_path TO {parent_id}")

        # verify switch
        verify = run_sql("SELECT current_schema();")
        print(verify)
        if not verify or verify[0][0] != parent_id:
            return "SECURITY ERROR: Failed to switch schema."
    try:
        res = run_sql(query=input)
        # Return structured result instead of repr for easier downstream handling
        if res is None:
            return "Query executed"
        # Convert rows to plain Python lists (JSON-serializable) if iterable
        try:
            return [list(r) for r in res]
        except Exception:
            return res
    except Exception:
        return "SQL error:\n" + traceback.format_exc()


TOOLS = [Run_Python, Run_SQL]


# -----------------------------
# Prompt for single Chat Agent
# -----------------------------
prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are XBase AI.\n"
        "You can answer normally OR use tools to run SQL & Python.\n"
        "You must ALWAYS respect the database schema:\n\n{db_info}\n\n"
        "If the user wants SQL, you may generate it and call Run_SQL.\n"
        "You can also run python via Run_Python.\n"
        "Use the following RAG context to improve SQL and reasoning:\n"
        "When summarizing tool results, absolutely do not call any tools. Return plain text only.\n"
        "{context}\n"
        "\nIMPORTANT:\n"
        "- Inlcude include the exact SQL command you intend to run in your reply, even if tools are disabled or execution is not confirmed.\n"
        "VERY IMPORTANT: If the user query contains the string 'DEV_NEEDS' then do not return anything other than the properly formatted csv, no natural language, no sql queries ONLY pure data should be returned as a list"
        "- Do NOT execute any SQL until the user explicitly confirms.\n"
        "- Confirmation flow:\n"
        "  1) First, present the exact SQL command you intend to run and ask: \"Do you want me to run this SQL? (Accept/Decline)\".\n"
        "  2) If the user replies Accept, then proceed to run the SQL via Run_SQL.\n"
        "  3) If the user replies Decline, do NOT run SQL and respond: \"Okay, I won't run it. Do you have any further queries?\".\n"
    ),
    MessagesPlaceholder("chat_history"),
    ("human", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])


# -----------------------------
# Summariser Agent (no tools)
# -----------------------------
summariser_prompt = ChatPromptTemplate.from_messages([
    ("system", "Summarise the user's query and the AI reply into one concise line. Output only the one-line summary."),
    ("human", "Query: {query}\nReply: {reply}\nOne-line summary:")
])
SUMMARISER = (summariser_prompt | chat_llm)

def summarise_interaction(query: str, reply: str) -> str:
    out = SUMMARISER.invoke({"query": query, "reply": reply})
    return getattr(out, "content", str(out)).strip().replace("\n", " ")

# -----------------------------
# Build unified Chat Agent
# -----------------------------
def build_chat_agent():
    # first retrieve context using only user query
    retrieval = RunnableParallel({
        "context": lambda x: retriever.invoke(x["input"]),
        "input": lambda x: x["input"],
        "db_info": lambda x: x["db_info"],
        "chat_history": lambda x: x.get("chat_history", []),   # keep list untouched
        "agent_scratchpad": lambda x: x.get("agent_scratchpad", []),  # keep list untouched
    })

    agent = (
        retrieval
        | prompt
        | chat_llm.bind_tools(TOOLS, tool_choice="auto")
    )

    return agent





CHAT_AGENT = build_chat_agent()


# -----------------------------
# Execute tools safely
# -----------------------------
def execute_tool(tool_obj, args):
    """Executes LC tool using .invoke or function call."""
    try:
        # LC tools use .invoke({"input": "..."})
        if hasattr(tool_obj, "invoke"):
            # args can be a dict for structured tools
            if isinstance(args, dict):
                out = tool_obj.invoke(args)
            else:
                out = tool_obj.invoke({"input": args})
            if hasattr(out, "content"):
                return out.content
            return out
        # Fallback direct call
        if isinstance(args, dict):
            return tool_obj(**args)
        return tool_obj(args)
    except Exception:
        return "Tool execution error:\n" + traceback.format_exc()


# -----------------------------
# CSV helpers for DEV_NEEDS
# -----------------------------
def _fetch_columns_for_table(table_name: str) -> list[str]:
    try:
        cols_res = run_sql(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;"
        )
        return [row[0] for row in cols_res] if cols_res else []
    except Exception:
        return []

def _fetch_rows_for_table(table_name: str) -> list[tuple]:
    try:
        rows_res = run_sql(f"SELECT * FROM {table_name};")
        return rows_res or []
    except Exception:
        return []

def _to_csv(columns: list[str], rows: list[tuple]) -> str:
    # Build a simple CSV string
    if not columns:
        if rows:
            columns = [f"col{i+1}" for i in range(len(rows[0]))]
        else:
            columns = []
    lines = []
    lines.append(",".join(columns))
    for r in rows:
        lines.append(",".join(map(str, r)))
    return "\n".join(lines)


# -----------------------------
# Schema helper for fallbacks
# -----------------------------
def _set_search_path(parent_id: str):
    try:
        target = "schema" + parent_id.replace('-', '_')
        run_sql(f"CREATE SCHEMA IF NOT EXISTS {target}")
        run_sql(f"SET search_path TO {target}")
    except Exception:
        pass


# -----------------------------
# MAIN FUNCTION YOU ASKED FOR
# -----------------------------
def Ask_AI(db_info: str, parent_id: str, query: str, chat_history=None, permission: bool = True, image_box: list[str] | None = None):
    """
    Unified function:
    - Injects db_info into system
    - Injects RAG context
    - Allows SQL & Python tool usage
    - Returns final assistant message
    - permission: if False, the AI must not use any tools
    """
    if chat_history is None:
        chat_history = []
    if image_box is None:
        image_box = []
    sql_res: list | None = []

    # First LLM call
    response = CHAT_AGENT.invoke({
        "input": query,
        "db_info": db_info,
        "chat_history": chat_history,
        "agent_scratchpad": [],
    })

    tool_calls = getattr(response, "tool_calls", [])

    # If tools are not permitted, ignore any tool calls and answer directly
    if permission is False:
        final = getattr(response, "content", str(response))
        chat_history.append(summarise_interaction(query, final))
        return final, chat_history, image_box, sql_res

    # If model didn't request any tool → return final answer
    if not tool_calls:
        final = getattr(response, "content", str(response))
        # store only one-line summary instead of full messages
        chat_history.append(summarise_interaction(query, final))
        return final, chat_history, image_box, sql_res

    # If model requests tools, run them and summarize
    results_text = ""
    tool_messages = []  # collect ToolMessage objects
    requested_sql_table = None

    for call in tool_calls:
        tool_name = call["name"]
        # pass through all args produced by the model/tool call
        call_args = dict(call["args"]) if isinstance(call.get("args"), dict) else {}
        tool_call_id = call["id"]

        tool_map = {t.name: t for t in TOOLS}
        tool_obj = tool_map[tool_name]

        # Build args for tool execution; include parent_id for Run_SQL
        exec_args = call_args
        if tool_name == "Run_SQL":
            exec_args["parent_id"] = parent_id
        elif tool_name == "Run_Python":
            # ensure image_box is passed so images can be collected
            exec_args.setdefault("image_box", image_box)

        out = execute_tool(tool_obj, exec_args)
        results_text += f"[{tool_name} OUTPUT]:\n{out}\n\n"

        # Collect SQL results: if Run_SQL returns rows, append; if 'Query executed' or None, append None
        if tool_name == "Run_SQL":
            # Normalize SQL output into sql_res list accurately
            if isinstance(out, str) and out.strip() == "Query executed":
                # Fallback: if the SQL looks like a SELECT, try fetching rows directly
                sql_text = exec_args.get("input", "")
                import re
                if re.match(r"^\s*select\b", sql_text, re.IGNORECASE):
                    m = re.search(r"from\s+([a-zA-Z0-9_]+)", sql_text, re.IGNORECASE)
                    table = m.group(1) if m else None
                    if table:
                        # ensure correct schema then fetch
                        _set_search_path(parent_id)
                        rows = _fetch_rows_for_table(table) or []
                        sql_res.append([list(r) for r in rows])
                    else:
                        sql_res.append(None)
                else:
                    sql_res.append(None)
            elif isinstance(out, (list, tuple)):
                sql_res.append(out)
            else:
                # try to eval repr safely into Python object when it's a string
                if isinstance(out, str):
                    try:
                        import ast
                        parsed = ast.literal_eval(out)
                        sql_res.append(parsed)
                    except Exception:
                        sql_res.append(None)
                else:
                    sql_res.append(None)

        # Create a ToolMessage that references the tool_call_id
        tool_messages.append(
            ToolMessage(
                content=out if isinstance(out, str) else repr(out),
                tool_call_id=tool_call_id,
            )
        )

        # Capture table name for DEV_NEEDS if available via SQL input
        if "DEV_NEEDS" in query and tool_name == "Run_SQL":
            sql_text = exec_args.get("input", "")
            import re
            m = re.search(r"FROM\s+([a-zA-Z0-9_]+)", sql_text, re.IGNORECASE)
            if m:
                requested_sql_table = m.group(1)

    # Second pass — include both the original assistant tool_call message and all tool messages
    # If DEV_NEEDS is requested, return CSV-only output
    if "DEV_NEEDS" in query:
        # Try to extract current table from db_info if not found from SQL
        table_name = requested_sql_table
        if not table_name:
            import re
            m2 = re.search(r"Current table is\s+([a-zA-Z0-9_]+)", str(db_info))
            if m2:
                table_name = m2.group(1)

        cols = _fetch_columns_for_table(table_name) if table_name else []
        rows = _fetch_rows_for_table(table_name) if table_name else []
        csv_text = _to_csv(cols, rows)
        chat_history.append(summarise_interaction(query, "CSV returned"))
        return csv_text, chat_history, image_box, sql_res

    final = CHAT_AGENT.invoke({
        "input": f"Summarize what happened:\n{results_text}",
        "db_info": db_info,
        "chat_history": chat_history,
        "agent_scratchpad": [response, *tool_messages],
    })

    # store only one-line summary instead of full messages
    chat_history.append(summarise_interaction(query, final.content))
    return final.content, chat_history, image_box, sql_res


# -----------------------------
# Interactive Chat Loop
# -----------------------------
if __name__ == "__main__":
    print("XBase AI Chat Running... (type 'stop' to exit)\n")

    db_info = "users(id INT, name TEXT, age INT);"  # <-- Replace with actual schema
    chat_history = []

    while True:
        try:
            user_input = input("You: ")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            break

        if user_input.strip().lower() == "stop":
            print("Stopping XBase AI.")
            break

        # Call Ask_AI function (toggle permission as needed)
        response = Ask_AI(
            db_info=db_info,
            parent_id="56103995-7437-499e-befd-eb6a6f12cb0e",  # replace with actual schema/parent_id
            query=user_input,
            chat_history=chat_history,
            permission=True  # set False to prevent any tool usage
        )

        print("\nAI:", response, "\n")
