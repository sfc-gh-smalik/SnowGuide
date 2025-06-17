import streamlit as st
import re
from snowflake.core import Root # requires snowflake>=0.8.0
from snowflake.snowpark.context import get_active_session

MODELS = [
    "claude-3-7-sonnet",
    "llama3-8b",
    "mistral-large"
]

def init_messages():
    """
    Initialize the session state for chat messages. If the session state indicates that the
    conversation should be cleared or if the "messages" key is not in the session state,
    initialize it as an empty list.
    """
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def init_service_metadata():
    """
    Initialize the session state for cortex search service metadata. Query the available
    cortex search services from the Snowflake session and store their names and search
    columns in the session state.
    """
    if "service_metadata" not in st.session_state:
        services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
        service_metadata = []
        if services:
            for s in services:
                svc_name = s["name"]
                svc_search_col = session.sql(
                    f"DESC CORTEX SEARCH SERVICE {svc_name};"
                ).collect()[0]["search_column"]
                service_metadata.append(
                    {"name": svc_name, "search_column": svc_search_col}
                )

        st.session_state.service_metadata = service_metadata


def init_config_options():
    """
    Initialize the configuration options in the Streamlit sidebar. Allow the user to select
    a cortex search service, clear the conversation, toggle debug mode, and toggle the use of
    chat history. Also provide advanced options to select a model, the number of context chunks,
    and the number of chat messages to use in the chat history.
    """
    st.sidebar.selectbox(
        "Select cortex search service:",
        [s["name"] for s in st.session_state.service_metadata],
        key="selected_cortex_search_service",
    )

    st.sidebar.button("Clear conversation", key="clear_conversation")
    st.sidebar.toggle("Debug", key="debug", value=False)
    st.sidebar.toggle("Use chat history", key="use_chat_history", value=True)

    with st.sidebar.expander("Advanced options"):
        st.selectbox("Select model:", MODELS, key="model_name")
        st.number_input(
            "Select number of context chunks",
            value=10,
            key="num_retrieved_chunks",
            min_value=1,
            max_value=20,
        )
        st.number_input(
            "Select number of messages to use in chat history",
            value=10,
            key="num_chat_messages",
            min_value=1,
            max_value=20,
        )

    st.sidebar.expander("Session State").write(st.session_state)


def hybrid_search(query, service, k=5):
    """
    Perform hybrid search combining semantic and keyword-based search results.
    """
    # Get search column name and URL column
    service_metadata = [s for s in st.session_state.service_metadata 
                       if s["name"] == st.session_state.selected_cortex_search_service][0]
    search_col = service_metadata["search_column"]
    
    # Semantic search using service.search()
    semantic_results = service.search(
        query,
        columns=[search_col, "url"],  # Add URL column
        limit=k
    ).results
    
    # Extract semantic search texts to avoid duplicates
    semantic_texts = [r[search_col] for r in semantic_results]
    
    # Additional keyword-based search
    keyword_results = service.search(
        query,
        columns=[search_col, "url"],  # Add URL column
        limit=k,
        strategy='keyword'
    ).results
    
    # Combine and deduplicate results
    combined_results = semantic_results + keyword_results
    seen_texts = set()
    final_results = []
    
    for r in combined_results:
        text = r[search_col]
        if text not in seen_texts and len(final_results) < k:
            final_results.append(r)
            seen_texts.add(text)
    
    return final_results


def query_cortex_search_service(query):
    """
    Modified to properly format context with URLs
    """
    db, schema = session.get_current_database(), session.get_current_schema()
    
    cortex_search_service = (
        root.databases[db]
        .schemas[schema]
        .cortex_search_services[st.session_state.selected_cortex_search_service]
    )

    results = hybrid_search(
        query, 
        cortex_search_service, 
        k=st.session_state.num_retrieved_chunks
    )

    service_metadata = st.session_state.service_metadata
    search_col = [s["search_column"] for s in service_metadata 
                 if s["name"] == st.session_state.selected_cortex_search_service][0]

    context_str = ""
    for i, r in enumerate(results):
        # Extract URL and ensure it's properly formatted
        url = r.get("url", "").strip()
        if not url:
            url = "#"  # Default URL if none provided
        
        # Format context with document number and URL
        context_str += f"Context document {i+1}: {r[search_col].strip()} [URL: {url}]\n\n"

    if st.session_state.debug:
        st.sidebar.text_area("Context documents", context_str, height=500)

    return context_str


def get_chat_history():
    """
    Retrieve the chat history from the session state limited to the number of messages specified
    by the user in the sidebar options.

    Returns:
        list: The list of chat messages from the session state.
    """
    start_index = max(
        0, len(st.session_state.messages) - st.session_state.num_chat_messages
    )
    return st.session_state.messages[start_index : len(st.session_state.messages) - 1]

def complete(model, prompt):
    """
    Generate a completion for the given prompt using the specified model.

    Args:
        model (str): The name of the model to use for completion.
        prompt (str): The prompt to generate a completion for.

    Returns:
        str: The generated completion.
    """
    return session.sql("SELECT snowflake.cortex.complete(?,?)", (model, prompt)).collect()[0][0]


def make_chat_history_summary(chat_history, question):
    """
    Generate a summary of the chat history combined with the current question to extend the query
    context. Use the language model to generate this summary, with citations removed.
    """
    # Clean chat history of citations before summarizing
    cleaned_history = []
    for msg in chat_history:
        content = msg["content"]
        # Remove citation markers [^1], [^2], etc.
        cleaned_content = re.sub(r'\[\^[0-9]+\]', '', content)
        # Remove the Sources section if it exists
        if "Sources:" in cleaned_content:
            cleaned_content = cleaned_content.split("Sources:")[0]
        cleaned_history.append({"role": msg["role"], "content": cleaned_content.strip()})
    
    # Format the cleaned history as a string
    history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in cleaned_history])
    
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.

        <chat_history>
        {history_str}
        </chat_history>
        <question>
        {question}
        </question>
        [/INST]
    """

    summary = complete(st.session_state.model_name, prompt)

    if st.session_state.debug:
        st.sidebar.text_area(
            "Chat history summary", summary.replace("$", "\$"), height=150
        )

    return summary


def create_prompt(user_question):
    """
    Create a prompt for the language model with instructions for inline citations.
    """
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history != []:
            question_summary = make_chat_history_summary(chat_history, user_question)
            prompt_context = query_cortex_search_service(question_summary)
        else:
            prompt_context = query_cortex_search_service(user_question)
    else:
        prompt_context = query_cortex_search_service(user_question)
        chat_history = ""

    prompt = f"""
            [INST]
            You are a helpful AI chat assistant with RAG capabilities. When a user asks you a question,
            you will also be given context provided between <context> and </context> tags. Use that context
            with the user's chat history provided in the between <chat_history> and </chat_history> tags
            to provide a summary that addresses the user's question. 

            Important instructions for your response:
            1. Use inline citations by adding [^#] after each statement where # is the context document number
            2. When multiple documents support a statement, include all relevant numbers [^1][^2]
            3. At the end of your response, include a "Sources:" section with full markdown links
            4. Format each source as "[^#] [Context document #](actual_url)" where # is the document number
            5. Extract URLs from context (found after [URL: ]) for the markdown links
            6. If you cannot answer with the given context, just say "I don't know the answer to that question"
            7. Don't say "according to the provided context"

            <chat_history>
            {chat_history}
            </chat_history>
            <context>
            {prompt_context}
            </context>
            <question>
            {user_question}
            </question>
            [/INST]
            Answer:
        """
    return prompt


def format_response(generated_response):
    """
    Format the response with clean styling for citations and content structure
    """
    # Split response into content and sources
    parts = generated_response.split("Sources:")
    content = parts[0].strip()
    sources = parts[1].strip() if len(parts) > 1 else ""
    
    # Process citations with markdown formatting
    processed_content = content
    
    # Format lists and paragraphs
    lines = processed_content.split("\n")
    formatted_lines = []
    in_list = False
    
    for line in lines:
        line = line.strip()
        if line:
            if line.startswith("- ") or line.startswith("* "):
                if not in_list:
                    formatted_lines.append("")
                    in_list = True
                formatted_lines.append(line)
            else:
                if in_list:
                    formatted_lines.append("")
                    in_list = False
                formatted_lines.append(line)
    
    processed_content = "\n".join(formatted_lines)
    
    # Combine the formatted response
    if sources:
        formatted_response = f"{processed_content}\n\n**Sources:**\n{sources}"
    else:
        formatted_response = processed_content
    
    return formatted_response



def main():
    st.title(f":speech_balloon: Snowflake Docs Chatbot")
    # Add CSS for citation styling
    st.markdown("""
        <style>
        /* General styles */
        sup {
            vertical-align: super;
            font-size: smaller;
            background-color: #f0f2f6;
            border-radius: 50%;
            padding: 2px 6px;
            margin: 0 2px;
        }
        .sources {
            margin-top: 20px;
            padding-top: 10px;
            border-top: 1px solid #eee;
        }
        a {
            color: #0068c9;
            text-decoration: none;
        }
        
        /* Code block styling */
        pre {
            background-color: #1e1e1e;
            padding: 1em;
            border-radius: 8px;
            overflow-x: auto;
        }
        code {
            color: #d4d4d4;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
        }
        
        /* SQL syntax highlighting */
        .sql-keyword { color: #569cd6; }  /* Keywords like SELECT, FROM */
        .sql-function { color: #dcdcaa; }  /* Functions */
        .sql-string { color: #ce9178; }   /* String literals */
        .sql-number { color: #b5cea8; }   /* Numbers */
        .sql-operator { color: #d4d4d4; } /* Operators */
        .sql-comment { color: #6a9955; }  /* Comments */
        
        /* Python syntax highlighting */
        .python-keyword { color: #569cd6; }   /* def, class, import */
        .python-string { color: #ce9178; }    /* String literals */
        .python-function { color: #dcdcaa; }  /* Function names */
        .python-class { color: #4ec9b0; }     /* Class names */
        .python-number { color: #b5cea8; }    /* Numbers */
        .python-comment { color: #6a9955; }   /* Comments */
        </style>
    """, unsafe_allow_html=True)


    init_service_metadata()
    init_config_options()
    init_messages()

    icons = {"assistant": "❄️", "user": "👤"}

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar=icons[message["role"]]):
            st.markdown(message["content"])

    disable_chat = (
        "service_metadata" not in st.session_state
        or len(st.session_state.service_metadata) == 0
    )
    if question := st.chat_input("Ask a question...", disabled=disable_chat):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": question})
        # Display user message in chat message container
        with st.chat_message("user", avatar=icons["user"]):
            st.markdown(question.replace("$", "\$"))

        # Display assistant response in chat message container
        with st.chat_message("assistant", avatar=icons["assistant"]):
            message_placeholder = st.empty()
            question = question.replace("'", "")
            with st.spinner("Thinking..."):
                generated_response = complete(
                    st.session_state.model_name, create_prompt(question)
                )

                formatted_response = format_response(generated_response)
                message_placeholder.markdown(formatted_response, unsafe_allow_html=True)

                # Split response into content and sources
                parts = generated_response.split("Sources:")
                content = parts[0]
                sources = parts[1] if len(parts) > 1 else ""
                
                # Process the main content - convert citations to superscript
                processed_content = content.replace("[^", "<sup>").replace("]", "</sup>")
                
                # Keep sources as markdown for proper link rendering
                processed_response = processed_content + "\nSources:" + sources if sources else processed_content
                
                message_placeholder.markdown(processed_response, unsafe_allow_html=True)

        st.session_state.messages.append(
            {"role": "assistant", "content": generated_response}
        )


if __name__ == "__main__":
    session = get_active_session()
    root = Root(session)
    main()
