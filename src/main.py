import streamlit as st
import openai
from langchain.llms import OpenAI

# Configure your OpenAI API key
openai.api_key = 'YOUR_OPENAI_API_KEY'

# Initialize LangChain's OpenAI LLM
llm = OpenAI(api_key=openai.api_key)

st.title("Chat with OpenAI")

if 'messages' not in st.session_state:
    st.session_state['messages'] = []

def generate_response(prompt):
    response = llm(prompt)
    return response.strip()

user_input = st.text_input("You: ", "")

if user_input:
    st.session_state['messages'].append(f"You: {user_input}")
    response = generate_response(user_input)
    st.session_state['messages'].append(f"AI: {response}")

for message in st.session_state['messages']:
    st.write(message)