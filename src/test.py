import warnings
warnings.filterwarnings('ignore')

import duckdb
import numpy as np
import pandas as pd
import os
from typing_extensions import TypedDict
from typing import List, Annotated
import matplotlib.pyplot as plt
import io
import base64

# Imports from LangGraph modules

from langchain_aws import ChatBedrockConverse
from langgraph.graph import StateGraph, END, START
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import SystemMessage, HumanMessage

from operator import add
from langchain_aws import ChatBedrock

model_id = "mistral.mistral-7b-instruct-v0:2"
#model_id = 'meta.llama3-2-1b-instruct-v1:0'
model_kwargs = {
    "max_tokens": 512,
    "temperature": 0,
    "top_k" : 50,
    "top_p": 0.9
}
model = ChatBedrock(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                      region_name=os.getenv('AWS_REGION', 'us-east-1'),
                                        model_id=model_id,
                                        model_kwargs=model_kwargs,)
# Set up the OpenAI API key (replace 'your-openai-api-key' with your actual key)
os.environ['OPENAI_API_KEY'] = 'put your api key here'

# Define the agent state using TypedDict
class AgentState(TypedDict):
    question: str
    table_schemas: str
    database: str
    sql: str
    reflect: Annotated[List[str], add]
    accepted: bool
    revision: int
    max_revision: int
    results: List[tuple]
    interpretation: str
    plot_needed: bool
    plot_html: str

# Function to get the database schema
def get_database_schema(db_path):
    conn = duckdb.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'")
    tables = cursor.fetchall()
    schema = ''
    for table_name in tables:
        table_name = table_name[0]
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        schema += f"Table: {table_name}\n"
        schema += "Columns:\n"
        for column in columns:
            schema += f" - {column[0]} ({column[1]})\n"
        schema += '\n'
    conn.close()
    return schema

# Define the nodes for LangGraph
def search_engineer_node(state: AgentState):
    db_schema = get_database_schema('my_dw.duckdb')
    state['table_schemas'] = db_schema
    state['database'] = 'my_dw.duckdb'
    return state

def sql_writer_node(state: AgentState):
    role_prompt = """
You are a DuckDB expert and its SQL syntax. Your task is to write **only** the SQL query that answers the user's question. The query must:

- Use standard DuckDB SQL syntax in English.
- Use table and column names as defined in the database schema.
- Not include comments, explanations, or any additional text.
- Not use code formatting or markdown.
- Return only the valid SQL query.
- WORK IN ALL UPPERCASE
- Work only with the empresa table
- USE LIKE in  WHERE
"""
    instruction = f"Database schema:\n{state['table_schemas']}\n"
    if len(state['reflect']) > 0:
        instruction += f"Consider the following feedback:\n{chr(10).join(state['reflect'])}\n"
    instruction += f"Write the SQL query that answers the following question: {state['question']}\n"
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    response = model.invoke(messages)
    state['sql'] = response.content.strip()
    state['revision'] += 1
    return state

def qa_engineer_node(state: AgentState):
    role_prompt = """
You are a QA engineer specialized in the DuckDB relational database and its SQL syntax. Your task is to verify if the provided SQL query correctly answers the user's question.
"""
    instruction = f"Based on the following database schema:\n{state['table_schemas']}\n"
    instruction += f"And the following SQL query:\n{state['sql']}\n"
    instruction += f"Verify if the SQL query can complete the task: {state['question']}\n"
    instruction += "Respond with 'ACCEPTED' if correct or 'REJECTED' if not.\n"
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    response = model.invoke(messages)
    state['accepted'] = 'ACCEPTED' in response.content.upper()
    return state

def chief_dba_node(state: AgentState):
    role_prompt = """
You are an experienced DBA, specialized in DuckDB. Your task is to provide detailed feedback to improve the provided SQL query.
"""
    instruction = f"Based on the following database schema:\n{state['table_schemas']}\n"
    instruction += f"And the following SQL query:\n{state['sql']}\n"
    instruction += f"Please provide useful and detailed recommendations to help improve the SQL query for the task: {state['question']}\n"
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    response = model.invoke(messages)
    state['reflect'].append(response.content)
    return state

def execute_query_node(state: AgentState):
    conn = duckdb.connect(state['database'])
    cursor = conn.cursor()
    try:
        cursor.execute(state['sql'])
        state['results'] = cursor.fetchall()
    except Exception as e:
        state['results'] = []
        state['error'] = str(e)
    finally:
        cursor.close()
        conn.close()
    return state

def interpret_results_node(state: AgentState):
    role_prompt = """
You are an assistant specialized in interpreting SQL query results with DuckDB syntax, explaining them in natural language, and suggesting appropriate visualizations. Avoid suggesting charts when the result is a single value.
Your task is to analyze the query results, provide a clear and concise answer to the user's original question, and suggest a suitable chart type if there is numerical data to compare with other units, locations, customers, etc.
"""
    instruction = f"Original question: {state['question']}\n"
    instruction += f"Executed SQL query: {state['sql']}\n"
    instruction += f"Query results: {state['results']}\n"
    instruction += "Please interpret these results, answer the original question in natural language, and suggest a suitable chart type if applicable, especially when there is numerical data and comparisons. If the answer is like 'The largest' or 'The most expensive', indicating a maximum unit, avoid plotting charts."
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    response = model.invoke(messages)
    state['interpretation'] = response.content
    # Decide if a chart is needed based on the interpretation
    if "chart" in response.content.lower() or "graph" in response.content.lower():
        state['plot_needed'] = True
    else:
        state['plot_needed'] = False
    return state

def plot_results_node(state: AgentState):
    if not state['plot_needed']:
        state['plot_html'] = ''
        return state
    # Generate the chart
    results = state['results']
    if not results or len(results) == 0:
        state['plot_html'] = ''
        return state
    # Assuming the first column is labels and the last column is values
    labels = [str(row[0]) for row in results]
    values = [row[-1] if isinstance(row[-1], (int, float)) else 0 for row in results]
    plt.figure(figsize=(10, 6))
    # Determine the chart type based on the suggestion
    if "bar" in state['interpretation'].lower():
        plt.bar(labels, values)
    elif "pie" in state['interpretation'].lower():
        plt.pie(values, labels=labels, autopct='%1.1f%%')
    else:  # default to line chart
        plt.plot(labels, values, marker='o')
    plt.title(state['question'])
    plt.xlabel('Categories')
    plt.ylabel('Values')
    plt.xticks(rotation=45)
    plt.tight_layout()
    # Save the chart to a buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    # Convert the chart to base64
    img_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
    # Create HTML to display the chart
    img_html = f'<img src="data:image/png;base64,{img_base64}">'
    state['plot_html'] = img_html
    return state

# Build the LangGraph
builder = StateGraph(AgentState)

# Add nodes to the graph
builder.add_node('search_engineer', search_engineer_node)
builder.add_node('sql_writer', sql_writer_node)
builder.add_node('qa_engineer', qa_engineer_node)
builder.add_node('chief_dba', chief_dba_node)
builder.add_node('execute_query', execute_query_node)
builder.add_node('interpret_results', interpret_results_node)
builder.add_node('plot_results', plot_results_node)

# Define the edges between nodes
builder.add_edge(START, 'search_engineer')
builder.add_edge('search_engineer', 'sql_writer')
builder.add_edge('sql_writer', 'qa_engineer')

# Conditional edge from qa_engineer
builder.add_conditional_edges(
    'qa_engineer',
    lambda state: 'execute_query' if state['accepted'] or state['revision'] >= state['max_revision'] else 'chief_dba',
    {'execute_query': 'execute_query', 'chief_dba': 'chief_dba'}
)

builder.add_edge('chief_dba', 'sql_writer')
builder.add_edge('execute_query', 'interpret_results')
builder.add_edge('interpret_results', 'plot_results')
builder.add_edge('plot_results', END)

# Set the entry point
builder.set_entry_point('search_engineer')

# Compile the graph with a checkpointer
memory = MemorySaver()
graph = builder.compile(checkpointer=memory)

# Function to process a question using the graph
def process_question(question):
    initial_state = {
        'question': question,
        'table_schemas': '',
        'database': '',
        'sql': '',
        'reflect': [],
        'accepted': False,
        'revision': 0,
        'max_revision': 2,
        'results': [],
        'interpretation': '',
        'plot_needed': False,
        'plot_html': ''
    }
    thread = {'configurable': {'thread_id': '1'}}
    for s in graph.stream(initial_state, thread):
        pass
    # Get the final state
    final_state = graph.get_state(thread).values

    # Print the results
    print('Generated SQL Query:\n', final_state['sql'])
    print('\nQuery Results:')
    if 'results' in final_state:
        for result in final_state['results']:
            print(result)
    else:
        print('No results')
    print('\nInterpretation:')
    print(final_state['interpretation'])
    #if final_state.get('plot_html'):
    #    display(HTML(final_state['plot_html']))
    #else:
        #print('No chart generated.')
question = "What is the CNPJ of the company vaccinar?"
process_question(question)