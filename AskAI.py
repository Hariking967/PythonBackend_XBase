from langchain_core.tools import tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables.base import RunnableSerializable
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
import os
from CRUD import run_sql 

# Load environment variables
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
llm = ChatOpenAI(openai_api_key=api_key, model="gpt-3.5-turbo", temperature=0.0)

# -----------------------
# Tools
# -----------------------
@tool
def Master_Agent(plan: str) -> str:
    """Master agent tool: receives a plan step, coordinates with Slave agent and tools."""
    return f"Master processed plan: {plan}"

@tool
def Slave_Agent(command: str) -> str:
    """Slave agent tool: receives a command and executes using available tools."""
    return f"Slave executed command: {command}"

@tool
def Run_Python(code: str) -> str:
    """Executes Python code safely in an isolated namespace and returns output or errors."""

@tool
def Run_SQL(query: str) -> str:
    result = run_sql(query=query)
    if (result is None):
        "Query executed"
    else:
        return result

# -----------------------
# Shared system prompt
# -----------------------
prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are {agent_role}. You have access to tools but you must use them "
        "ONLY when absolutely necessary. Prefer normal reasoning and answering "
        "unless a tool is required to execute code or run SQL.\n\n"
        "**Database Info Provided by User:**\n{db_info}\n\n"
        "Rules:\n"
        "1. Respond normally unless a tool is needed to fulfill a user request.\n"
        "2. When using tools, think step-by-step and write clear arguments.\n"
        "3. Use the scratchpad to read tool outputs. If the scratchpad already contains "
        "the answers, do NOT call more tools.\n"
        "4. Never hallucinate database schema — rely ONLY on the DB info string.\n"
        "Role:"
        "{detailed_role_description}"
    ),
    MessagesPlaceholder(variable_name="chat_history"),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

# -----------------------
# Agents wiring
# -----------------------
def agent_input_mapper(role):
    """Adds db_info & agent_role safely into every agent invocation."""
    if role == "Chat Agent":
        drd = (
            "You are the Chat Agent. "
            "Speak professionally to the user. "
            "Use db_info ONLY for answering simple factual queries. "
            "If the user asks to read, write, update, delete, or inspect the database, "
            "DO NOT do it yourself. Instead, call the Master Agent. "
            "Never execute Python or SQL directly."
        )

    elif role == "Master Agent":
        drd = (
            "You are the Master Agent. "
            "Your job is to create plans, make decisions, and orchestrate workflow. "
            "When a task involves Python or SQL execution, send the request to the Slave Agent. "
            "Use results returned by the Slave Agent to take further decisions or complete the task. "
            "You NEVER talk to the end-user directly."
        )

    elif role == "Slave Agent":
        drd = (
            "You are the Slave Agent. "
            "You execute Python code and SQL queries when the Master Agent instructs you. "
            "Return results immediately back to the Master Agent with no additional reasoning. "
            "Do not make plans or talk to the user—only execute and respond."
        )

    return {
        "input": lambda x: x["input"],
        "chat_history": lambda x: x.get("chat_history", []),
        "agent_scratchpad": lambda x: x.get("agent_scratchpad", []),
        "agent_role": lambda x: role,
        "db_info": lambda x: x.get("db_info", "No DB info provided."),
        "detailed_role_description": lambda x: drd
    }


# Master agent: uses Slave + Python + SQL tools
master_tools = [Slave_Agent, Run_Python, Run_SQL]
master_agent: RunnableSerializable = (
    agent_input_mapper("Master Agent")
    | prompt
    | llm.bind_tools(master_tools, tool_choice="auto")
)

# Slave agent: uses Master + Python + SQL tools
slave_tools = [Master_Agent, Run_Python, Run_SQL]
slave_agent: RunnableSerializable = (
    agent_input_mapper("Slave Agent")
    | prompt
    | llm.bind_tools(slave_tools, tool_choice="auto")
)

# Chat agent: uses all tools but prefers normal conversation
chat_tools = [Master_Agent, Slave_Agent, Run_Python, Run_SQL]
chat_agent: RunnableSerializable = (
    agent_input_mapper("Chat Agent")
    | prompt
    | llm.bind_tools(chat_tools, tool_choice="auto")
)

# -----------------------
# Example loop (optional)
# -----------------------
if __name__ == "__main__":
    while True:
        q = input("Ask: ")
        if q.strip().lower() == "stop":
            break

        db_info = "users(id INT, name TEXT);"  # Example; replace with real user input

        # Invoke chat_agent first
        first = chat_agent.invoke({
            "input": q,
            "chat_history": [],
            "db_info": db_info
        })

        print(first.tool_calls)

        name2tool = {t.name: t.func for t in chat_tools}

        if first.tool_calls:
            call = first.tool_calls[0]
            tool_exec_content = name2tool[call["name"]](**call["args"])
            print(tool_exec_content)

            from langchain_core.messages import ToolMessage
            tool_msg = ToolMessage(
                content=f"The {call['name']} tool returned {tool_exec_content}",
                tool_call_id=call["id"],
            )

            final = chat_agent.invoke({
                "input": "Summarize the tool result and next step.",
                "chat_history": [],
                "agent_scratchpad": [first, tool_msg],
                "db_info": db_info
            })
            print(final.content)

        else:
            print(first.content)
