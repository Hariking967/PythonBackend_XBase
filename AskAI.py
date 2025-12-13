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
def Run_Python(input: str) -> str:
    """Executes Python code and returns stdout or errors."""
    code = input
    local_ns = {}
    stdout = io.StringIO()
    try:
        old = sys.stdout
        sys.stdout = stdout
        exec(code, {"__name__": "__main__"}, local_ns)
        sys.stdout = old
        result = stdout.getvalue()
        if not result.strip() and local_ns:
            return repr(local_ns)
        return result
    except Exception:
        sys.stdout = old
        return "Python error:\n" + traceback.format_exc()


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
        if not verify or verify[0][0] != parent_id:
            return "SECURITY ERROR: Failed to switch schema."
    try:
        res = run_sql(query=input)
        return "Query executed" if res is None else repr(res)
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
        "- Always include the exact SQL command you intend to run in your reply, even if tools are disabled or execution is not confirmed.\n"
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
# MAIN FUNCTION YOU ASKED FOR
# -----------------------------
def Ask_AI(db_info: str, parent_id: str, query: str, chat_history=None, permission: bool = True):
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
        return final

    # If model didn't request any tool → return final answer
    if not tool_calls:
        final = getattr(response, "content", str(response))
        # store only one-line summary instead of full messages
        chat_history.append(summarise_interaction(query, final))
        return final

    # If model requests tools, run them and summarize
    results_text = ""
    tool_messages = []  # collect ToolMessage objects

    for call in tool_calls:
        tool_name = call["name"]
        tool_arg = call["args"].get("input", "")
        tool_call_id = call["id"]

        tool_map = {t.name: t for t in TOOLS}
        tool_obj = tool_map[tool_name]

        # Build args for tool execution; include parent_id for Run_SQL
        exec_args = {"input": tool_arg}
        if tool_name == "Run_SQL":
            exec_args["parent_id"] = parent_id

        out = execute_tool(tool_obj, exec_args)
        results_text += f"[{tool_name} OUTPUT]:\n{out}\n\n"

        # Create a ToolMessage that references the tool_call_id
        tool_messages.append(
            ToolMessage(
                content=out if isinstance(out, str) else repr(out),
                tool_call_id=tool_call_id,
            )
        )

    # Second pass — include both the original assistant tool_call message and all tool messages
    final = CHAT_AGENT.invoke({
        "input": f"Summarize what happened:\n{results_text}",
        "db_info": db_info,
        "chat_history": chat_history,
        "agent_scratchpad": [response, *tool_messages],
    })

    # store only one-line summary instead of full messages
    chat_history.append(summarise_interaction(query, final.content))
    return final.content, chat_history


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
