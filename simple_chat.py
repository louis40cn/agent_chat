import streamlit as st
import time
import random
from http import HTTPStatus
from dashscope import Application
import agent_call
from BailianLKE import BailianLKE

api_key = st.secrets["dashscope"]["api_key"]

BAILIAN_API_SECRET_ID = st.secrets["bailian"]["BAILIAN_API_SECRET_ID"]
BAILIAN_API_SECRET_KEY = st.secrets["bailian"]["BAILIAN_API_SECRET_KEY"]
BAILIAN_ENDPOINT = st.secrets["bailian"]["BAILIAN_ENDPOINT"]
BAILIAN_WORKSPACEID = st.secrets["bailian"]["BAILIAN_WORKSPACEID"]

BAILIAN_AGENT_LIST = [
    {
        'agent_name': '问小巴（文件问答）',
        'agent_id': '8f0b8bf23a5647bd9e8af632890525ff',
        'model_id': 'qwen-long',
    },
    {
        'agent_name': '问小巴（VIP）',
        'agent_id': '7d07727f0ef54bfd806e3a83a66c0c93',
        'model_id': 'deepseek-r1',
    },
]

# 前置工作流agent_id
PRECALL_AGENT_ID = "f7ca541c031445008a4e72b623562384"

bailian_client = BailianLKE(
        BAILIAN_API_SECRET_ID,
        BAILIAN_API_SECRET_KEY,
        BAILIAN_WORKSPACEID,
        BAILIAN_ENDPOINT
    )

# 1. 初始化会话状态变量和应用配置
# Set page config for wide layout and page title
st.set_page_config(page_title="问小巴测试", layout="wide")

# Initialize session state variables if they don't exist
curr_index = 0
if "messages" not in st.session_state:
    st.session_state.messages = []  # Stores the history of chat messages for the CURRENT session
if "api_key" not in st.session_state:
    st.session_state.api_key = st.secrets["dashscope"]["api_key"]
if "bailian_agent_id" not in st.session_state: # Specific Agent ID for Bailian
    st.session_state.bailian_agent_id = BAILIAN_AGENT_LIST[curr_index]["agent_id"]
if "chat_sessions" not in st.session_state:
    st.session_state.chat_sessions = [] # Stores past chat sessions {name: str, messages: list, id: str}
if "active_session_id" not in st.session_state: 
    st.session_state.active_session_id = None
if "selected_agent_index" not in st.session_state:
    st.session_state.selected_agent_index = curr_index
    st.session_state.agent_name = BAILIAN_AGENT_LIST[curr_index]['agent_name']
    st.session_state.model_id = BAILIAN_AGENT_LIST[curr_index]['model_id']
if "session_file_ids" not in st.session_state:
    st.session_state.session_file_ids = [] 

# --- 侧边栏 (Sidebar) ---
with st.sidebar:
    st.title("问小巴测试")
    st.markdown("---")

    model_display_names = [agent['agent_name'] for agent in BAILIAN_AGENT_LIST]

    selected_model_display_name = st.selectbox(
        "选择应用:",
        options=model_display_names,
        index=st.session_state.selected_agent_index,
        key="model_selector"
    )
    curr_index = model_display_names.index(selected_model_display_name)
    if st.session_state.selected_agent_index != curr_index:
        st.session_state.selected_agent_index = curr_index
        st.session_state.bailian_agent_id = BAILIAN_AGENT_LIST[curr_index]['agent_id']
        st.session_state.agent_name = BAILIAN_AGENT_LIST[curr_index]['agent_name']
        st.session_state.model_id = BAILIAN_AGENT_LIST[curr_index]['model_id']
        st.session_state.messages = [] 
        st.session_state.active_session_id = None
        st.session_state.session_file_ids = []
        st.rerun()

    st.markdown("---")
    if st.button("开启新会话"):
        st.session_state.messages = [] 
        st.session_state.active_session_id = None
        st.session_state.session_file_ids = []
        st.rerun()

# --- 主聊天界面 (Main Chat Interface) ---
st.caption(f"#### 当前应用: {st.session_state.agent_name}")

# 当前会话内容
for message in st.session_state.messages:
    with st.chat_message(message['role']):
        if "thoughts" in message and len(message['thoughts'])>0:
            with st.status("思考完成", state="complete"):
                st.markdown(message['thoughts'])
        st.markdown(message['content'])
        if "files" in message and len(message['files'])>0:
            st.write(f"文档：{'、'.join(message['files'])}")
        if "doc_references" in message and message["doc_references"]:
            with st.expander("引用列表", expanded=False):
                for ref in message["doc_references"]:
                    with st.expander(f"{ref['title']}@《{ref['doc_name']}》"):
                        st.write(ref['text'])

# 处理用户输入
if prompt := st.chat_input(
        "请输入您的问题...", 
        key="chat_input_main", 
        accept_file=True, 
        file_type=["pdf", "doc", "docx", "txt", "xls", "xlsx", "wps", "ppt", "pptx", "md"]
    ):

    st.session_state.messages.append({"role":"user", "content":prompt.text, "files":[fileobj.name for fileobj in prompt["files"]]})
    with st.chat_message("user"):
        st.markdown(prompt.text)

    with st.chat_message("assistant"):
        curr_model = st.session_state.model_id

        thinking_status = st.status("", expanded=True)

        if prompt["files"]:
            thinking_status.update(label="解析文件...", state="running", expanded=True)    
            for fileobj in prompt["files"]:
                try:
                    # 上传文件
                    result = bailian_client.TransferUploadedFileFromStreamlit(fileobj, tags=[], CategoryId="default", CategoryType="SESSION_FILE")
                    file_id = result['FileId']
                    st.session_state.session_file_ids.append(file_id)
                    while True:
                        result = bailian_client.DescribeDocument(file_id)
                        if result['Status'] == 'FILE_IS_READY':
                            thinking_status.write(f"**{fileobj.name}** ready")
                            break
                        if result['Status'] in ('PARSE_FAILED', 'SAFE_CHECK_FAILED', 'INDEX_BUILDING_FAILED', 'FILE_EXPIRED'):
                            thinking_status.write(f"解析文件**{fileobj.name}**失败，{result['Status']}")
                            break
                        time.sleep(2)
                except Exception as e:
                    thinking_status.write(f"解析文件**{fileobj.name}**异常，{e}")

        thinking_status.update(label="思考中...", state="running", expanded=True)    

        try:
            biz_params = None
            # 调用前置工作流智能体
            pre_agent_resp = agent_call.simple_agent_call(api_key, PRECALL_AGENT_ID, prompt.text)
            if pre_agent_resp:
                biz_params = {
                    "user_prompt_params": {
                        "估值参考": pre_agent_resp
                    }
                }

            # 发起主智能体调用
            responses = Application.call(
                api_key=st.session_state.api_key,
                app_id=st.session_state.bailian_agent_id,
                prompt=prompt.text,
                session_id=st.session_state.active_session_id,
                stream=True,
                incremental_output=True,
                has_thoughts=True,
                biz_params=biz_params,
                rag_options={
                    "session_file_ids":st.session_state.session_file_ids
                })   
        except Exception as e:
            full_response = f"请求异常：{e}"
            st.session_state.messages.append({"role":"assistant", "content":full_response})
            st.rerun()

        thinking_placeholder = thinking_status.empty()
        message_placeholder = st.empty()
        full_thoughts = ""
        full_response = ""
        doc_references = None
        thinking_completed = False
        for chunk in responses:
            # 跳过空数据块
            if not chunk.output or (not chunk.output.thoughts and not chunk.output.text):
                continue

            if chunk.status_code == HTTPStatus.OK:
                if chunk.output.thoughts:
                    # 在st.status容器中显示思考过程
                    for it in chunk.output.thoughts:
                        if it.action_type == "reasoning":
                            content = str(it.thought) if not isinstance(it.thought, str) else it.thought
                            full_thoughts += (content or "")
                            thinking_placeholder.markdown(full_thoughts + "|")

                if chunk.output.text:
                    if not thinking_completed:
                        # 更新思考完成状态
                        thinking_completed = True
                        thinking_placeholder.markdown(full_thoughts)
                        thinking_status.update(label="思考完成", state="complete", expanded=False)
                    # 显示结果
                    full_response += (chunk.output.text or "")
                    st.session_state.active_session_id = chunk.output.session_id
                    message_placeholder.markdown(full_response + "|")

                # if chunk.output.doc_references:
                #     with st.expander("引用列表", expanded=True):
                #         for ref in chunk.output.doc_references:
                #             with st.expander(f"{ref['title']}@《{ref['doc_name']}》"):
                #                 st.write(ref['text'])
                #     doc_references = chunk.output.doc_references
                    
            else:
                full_response = f"请求异常：code={chunk.status_code}, message={chunk.message}"
                break
        message_placeholder.markdown(full_response)

    st.session_state.messages.append({
        "role":"assistant", 
        "content":full_response, 
        "thoughts":full_thoughts, 
        })
    st.rerun()



