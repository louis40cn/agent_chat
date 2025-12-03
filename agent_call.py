import streamlit as st
from http import HTTPStatus
from dashscope import Application

api_key = st.secrets["dashscope"]["api_key"]

# 简单智能体调用，用于调用前置工作流智能体
def simple_agent_call(api_key, agent_id, prompt):
    response = Application.call(
        api_key=api_key,
        app_id=agent_id,
        prompt=prompt
    )

    if response.status_code != HTTPStatus.OK:
        return f"simple_agent_call请求异常：code={response.status_code}, message={response.message}"
    return response.output.text.strip()
