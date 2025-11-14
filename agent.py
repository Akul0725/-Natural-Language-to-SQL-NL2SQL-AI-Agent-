import os
from typing import TypedDict, Annotated, List
from langchain_groq import ChatGroq
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.graph import StateGraph, END

class AgentState(TypedDict):
    question: str
    db_uri: str
    schema: str
    sql_query: str
    sql_result: str
    answer: str
    error: str

def get_schema(state: AgentState):
    """Connects to the database and gets its schema."""
    print("---(Node: get_schema)---")
    try:
        db = SQLDatabase.from_uri(state['db_uri'])
        schema = db.get_table_info()
        return {"schema": schema, "error": None}
    except Exception as e:
        print(f"Error getting schema: {e}")
        return {"error": f"Error getting schema: {e}"}

def generate_sql(state: AgentState):
    """Generates the SQL query from the user question and schema."""
    print("---(Node: generate_sql)---")
    if state.get("error"): return {} 

    llm = ChatGroq(temperature=0, model_name="llama-3.3-70b-versatile")
    
    prompt = f"""
    Based on the database schema below, write a single, syntactically correct PostgreSQL query
    to answer the following question. Do not explain the query, just return the SQL.
    Schema:
    {state['schema']}
    Question:
    {state['question']}
    SQL Query:
    """
    
    try:
        msg = llm.invoke(prompt)
        sql_query = msg.content.strip()
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        return {"sql_query": sql_query.strip(), "error": None}
    except Exception as e:
        print(f"Error generating SQL: {e}")
        return {"error": f"Error generating SQL: {e}"}

def execute_sql(state: AgentState):
    """Executes the SQL query on the database."""
    print("---(Node: execute_sql)---")
    if state.get("error"): return {}

    try:
        db = SQLDatabase.from_uri(state['db_uri'])
        result = db.run(state['sql_query'])
        return {"sql_result": str(result), "error": None}
    except Exception as e:
        print(f"Error executing SQL: {e}")
        return {"error": f"Error executing SQL: {e}. Check your query: {state['sql_query']}"}

def generate_answer(state: AgentState):
    """Generates a natural language answer from the SQL result."""
    print("---(Node: generate_answer)---")
    if state.get("error"):
        prompt = f"""
        An error occurred trying to answer the question: "{state['question']}"
        The error was:
        {state['error']}
        Please explain this error to the user in a helpful, friendly way.
        """
    else:
        prompt = f"""
        The user asked: "{state['question']}"
        We ran the SQL query: "{state['sql_query']}"
        And got this result: "{state['sql_result']}"
        Please provide a concise, natural language answer to the user
        based on this information.
        """
    
    llm = ChatGroq(temperature=0, model_name="llama-3.3-70b-versatile")
    msg = llm.invoke(prompt)
    return {"answer": msg.content}


# --- Build the Graph ---

# --- REFACTORED THIS SECTION ---
def get_compiled_app():
    """
    Builds and compiles the LangGraph app.
    Returns the compiled app.
    """
    workflow = StateGraph(AgentState)
    workflow.add_node("get_schema", get_schema)
    workflow.add_node("generate_sql", generate_sql)
    workflow.add_node("execute_sql", execute_sql)
    workflow.add_node("generate_answer", generate_answer)
    workflow.set_entry_point("get_schema")
    workflow.add_edge("get_schema", "generate_sql")
    workflow.add_edge("generate_sql", "execute_sql")
    workflow.add_edge("execute_sql", "generate_answer")
    workflow.add_edge("generate_answer", END) 
    
    app = workflow.compile()
    return app

def run_agent_graph(question: str, db_uri: str):
    """
    Compiles and runs the LangGraph agent.
    """
    app = get_compiled_app()
    
    initial_state = {"question": question, "db_uri": db_uri}
    final_state = app.invoke(initial_state)

    return final_state.get("answer", "I'm sorry, I encountered an error and couldn't process your request.")
