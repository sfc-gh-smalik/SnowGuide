import re
import snowflake.connector
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from snowflake.core import Root
import atexit

MODELS = [
    # "claude-3-5-sonnet",
    "claude-3-7-sonnet",
]

load_dotenv()


def get_private_key():
    """Load private key from file and return it in the correct format"""
    try:
        with open(os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'], 'rb') as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None  # If your key has a password, provide it here
            )
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    except Exception as e:
        print(f"Error loading private key: {str(e)}")
        raise


class SlackChatBot:
    def __init__(self):
        # Initialize Snowflake connection using JWT
        try:
            self.conn = snowflake.connector.connect(
                user=os.environ['SNOWFLAKE_USER'],
                authenticator='SNOWFLAKE_JWT',
                private_key_file=os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'],
                account=os.environ['SNOWFLAKE_ACCOUNT'],
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                role=os.getenv("SNOWFLAKE_USER_ROLE"),
                host=os.getenv("HOST"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA")
            )
        except Exception as e:
            # fail over to external browser
            self.conn = snowflake.connector.connect(
                account=os.environ['SNOWFLAKE_ACCOUNT'],
                user=os.environ['SNOWFLAKE_USER'],
                private_key_path=get_private_key(),
                database=os.environ['SNOWFLAKE_DATABASE'],
                schema=os.environ['SNOWFLAKE_SCHEMA'],
                host=os.environ['HOST'],
                port=443,
                authenticator="externalbrowser",
                session_parameters={
                    'ABORT_DETACHED_QUERY': 'TRUE'}
            )

        self.cursor = self.conn.cursor()

        self.messages = {}  # Dict to store messages per channel
        self.model_name = MODELS[0]
        self.num_retrieved_chunks = 10
        self.num_chat_messages = 10
        self.use_chat_history = True
        self.debug = False
        self.service_metadata = self._init_service_metadata()

        if self.service_metadata:
            self.selected_service = self.service_metadata[0]["name"]

        # Initialize Slack app
        self.app = App(token=os.environ["SLACK_BOT_TOKEN"])

        # Send welcome message when bot joins a channel
        @self.app.event("member_joined_channel")
        def handle_member_joined_channel(event, say):
            if event.get("user") == self.app.client.auth_test()["user_id"]:
                self.send_welcome_message(event["channel"], say)

        self.setup_slack_handlers()
        self.root = Root(self.conn)
        # Store channel IDs for cleanup
        self.active_channels = set()

        # Register cleanup method
        atexit.register(self.cleanup)

    def _init_service_metadata(self):

        env_vars = [os.getenv("SNOWFLAKE_DATABASE"), os.getenv("SNOWFLAKE_SCHEMA")]
        f_schema_name = ".".join(env_vars)

        self.cursor.execute(f"USE SCHEMA {f_schema_name}")
        self.cursor.execute("SHOW CORTEX SEARCH SERVICES IN SCHEMA")
        services = self.cursor.fetchall()
        service_metadata = []
        if services:
            for s in services:
                svc_name = s[1]  # name column in show services
                self.cursor.execute(f"DESC CORTEX SEARCH SERVICE {svc_name}")
                svc_search_col = self.cursor.fetchone()[5]  # search_column in desc
                print(svc_search_col)
                # svc_search_col = 'content' # for DemoDB
                # svc_search_col = 'chunked_data'  # for snowhouse
                service_metadata.append(
                    {"name": svc_name, "search_column": svc_search_col}
                )
        return service_metadata

    # def hybrid_search(self, query, service, k=5):
    #     """Perform hybrid search combining semantic and keyword-based search"""
    #     service_metadata = [s for s in self.service_metadata
    #                         if s["name"] == self.selected_service][0]
    #     search_col = service_metadata["search_column"]
    #
    #     # Semantic search
    #     semantic_results = service.search(
    #         query,
    #         columns=[search_col, "URL"],
    #         limit=k
    #     ).results
    #
    #     # Keyword search
    #     keyword_results = service.search(
    #         query,
    #         columns=[search_col, "URL"],
    #         limit=k,
    #         strategy='keyword'
    #     ).results
    #
    #     # Combine and deduplicate results
    #     combined_results = semantic_results + keyword_results
    #     seen_texts = set()
    #     final_results = []
    #
    #     for r in combined_results:
    #         text = r[search_col]
    #         if text not in seen_texts and len(final_results) < k:
    #             final_results.append(r)
    #             seen_texts.add(text)
    #
    #     return final_results

    def hybrid_search(self, query, service, k=5):
        """
        Perform hybrid search combining semantic and keyword-based search with scoring

        Args:
            query (str): Search query
            service: Cortex search service instance
            k (int): Number of results to return

        Returns:
            list: Combined and deduplicated search results
        """
        service_metadata = [s for s in self.service_metadata
                            if s["name"] == self.selected_service][0]
        search_col = service_metadata["search_column"]

        # Perform semantic search with scoring
        semantic_results = service.search(
            query,
            columns=[search_col],
            limit=k * 2,  # Get more results for better combination
            strategy='semantic'
        ).results

        # Perform keyword search with scoring
        keyword_results = service.search(
            query,
            columns=[search_col],
            limit=k * 2,  # Get more results for better combination
            strategy='keyword'
        ).results

        # Combine results with deduplication and scoring
        seen_texts = {}
        final_results = []

        # Process semantic results first (typically higher quality)
        for i, r in enumerate(semantic_results):
            text = r[search_col]
            if text not in seen_texts:
                # Add semantic score boost
                r['_score'] = float(r.get('_score', 0)) * 1.2  # 20% boost for semantic results
                seen_texts[text] = r

        # Process keyword results
        for i, r in enumerate(keyword_results):
            text = r[search_col]
            if text not in seen_texts:
                seen_texts[text] = r
            else:
                # If exists, keep the higher score
                existing_score = float(seen_texts[text].get('_score', 0))
                new_score = float(r.get('_score', 0))
                if new_score > existing_score:
                    seen_texts[text] = r

        # Sort by score and take top k results
        final_results = sorted(
            seen_texts.values(),
            key=lambda x: float(x.get('_score', 0)),
            reverse=True
        )[:k]

        return final_results

    # def query_cortex_search_service(self, query):
    #     """Query the Cortex search service and format results"""
    #     db = os.getenv("SNOWFLAKE_DATABASE")
    #     schema = os.getenv("SNOWFLAKE_SCHEMA")
    #
    #     service = (self.root.databases[db]
    #     .schemas[schema]
    #     .cortex_search_services[self.selected_service])
    #
    #     results = self.hybrid_search(query, service, k=self.num_retrieved_chunks)
    #
    #     search_col = [s["search_column"] for s in self.service_metadata
    #                   if s["name"] == self.selected_service][0]
    #
    #     context_str = ""
    #     citations = []
    #     for i, r in enumerate(results):
    #         url = r.get("URL", "").strip() or "#"
    #         context_str += f"Context document {i + 1}: {r[search_col].strip()} [URL: {url}]\n\n"
    #
    #     if self.debug:
    #         print("Context documents:", context_str)
    #
    #     print('results', results)
    #
    #     if results:
    #         first_result = results[0]
    #         citations = [{
    #             "index": 1,
    #             "title": first_result.get("document_title", "Unknown Title"),
    #             "source": first_result.get("source_url", "Unknown Source")
    #         }]
    #
    #     print(context_str, citations)
    #     return context_str, citations

    def query_cortex_search_service(self, query):
        db = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")

        cortex_search_service = (
            self.root.databases[db]
            .schemas[schema]
            .cortex_search_services[self.selected_service]
        )

        context_documents = cortex_search_service.search(
            query,
            columns=["chunk", "document_title", "source_url"],
            limit=self.num_retrieved_chunks
        )
        results = context_documents.results

        search_col = [s["search_column"] for s in self.service_metadata
                      if s["name"] == self.selected_service][0]

        context_str = ""
        citations = []
        seen_urls = set()  # Track unique URLs

        for i, r in enumerate(results):
            content = None
            for col_name in [search_col, "chunk", "CHUNK", "content", "CONTENT"]:
                if col_name in r:
                    content = r[col_name]
                    break

            if content is None:
                if self.debug:
                    print(f"Could not find content in result {i + 1}. Available keys: {list(r.keys())}")
                content = f"Content not found - available keys: {list(r.keys())}"

            url = r.get("source_url", "Unknown Source")
            if url not in seen_urls:
                seen_urls.add(url)
                citations.append({
                    "index": len(citations) + 1,
                    "title": r.get("document_title", "Unknown Title"),
                    "source": url
                })

            context_str += f"Context document {i + 1}: {content} [URL: {url}]\n\n"

        return context_str, citations

    def complete(self, prompt):
        """Execute completion and handle multiple responses"""
        self.cursor.execute(
            "SELECT snowflake.cortex.complete(%s, %s)",
            (self.model_name, prompt)
        )
        result = self.cursor.fetchone()
        # Return the first element of the tuple if result exists
        return result[0] if result else None

    def send_welcome_message(self, channel_id, say):
        """Send welcome message with bot options and shortcuts"""
        welcome_blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Welcome to SnowGuide ! 👋",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "I'm here to help answer your questions about Snowflake using the available documentation."
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Available Commands:*\n• Type a question to get started\n• Type `/clear` or use the button below to clear chat history\n• Type `/end` or use the button below to end session and clear all messages"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Clear History 🧹",
                            "emoji": True
                        },
                        "value": "clear_history",
                        "action_id": "clear_history"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "End Session 👋",
                            "emoji": True
                        },
                        "style": "danger",
                        "value": "end_session",
                        "action_id": "end_session"
                    }
                ]
            },
            {"type": "divider"}
        ]
        say(blocks=welcome_blocks)

    def setup_slack_handlers(self):
        @self.app.event("message")
        def handle_message(event, say):
            if "bot_id" in event:  # Ignore bot messages
                return

            # Check if message has text content
            if "text" not in event:
                return

            channel_id = event["channel"]
            question = event["text"].lower().strip()

            # Track active channels for cleanup
            self.active_channels.add(channel_id)

            # Send welcome message if this is a new channel
            if channel_id not in self.messages:
                self.send_welcome_message(channel_id, say)
                self.messages[channel_id] = []

            # Rest of the existing message handling code...
            thinking_response = say({
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Let me think about that... :thinking_face:"
                        }
                    },
                    {
                        "type": "image",
                        "title": {
                            "type": "plain_text",
                            "text": "Thinking..."
                        },
                        "block_id": "thinking_gif",
                        "image_url": "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExcDdtY2MyMXBwOThuNXh5bXp1Ym5wbGprZnB3aWJteDNyYndxYjh6eCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9cw/tXL4FHPSnVJ0A/source.gif",
                        "alt_text": "A minimal loading animation"
                    }
                ]
            })

            # Initialize channel history if needed
            if channel_id not in self.messages:
                self.messages[channel_id] = []

            context, citations = self.query_cortex_search_service(question)

            prompt = f"""
            [INST]
            You are a helpful AI chat assistant. Use the context provided between 
            <context> and </context> tags to answer the user's question.

            Important instructions:
            1. Use inline citations [^#] after each statement
            2. Include multiple citations [^1][^2] when applicable
            3. Add a "Sources:" section at the end with full markdown links
            4. Format sources as "[^#] [Context document #](actual_url)"
            5. If you cannot answer, say "I don't know the answer to that question"

            <context>
            {context}
            </context>
            <citation>
            {citations}
            </citation>
            <question>
            {question}
            </question>
            [/INST]
            """

            if channel_id not in self.messages:
                self.messages[channel_id] = []

            context, citations = self.query_cortex_search_service(question)
            response = self.complete(prompt)

            # Delete thinking message
            try:
                self.app.client.chat_delete(
                    channel=channel_id,
                    ts=thinking_response['ts']
                )
            except Exception as e:
                print(f"Error deleting thinking message: {e}")

            if response:
                self.messages[channel_id].append({"role": "user", "content": question})
                self.messages[channel_id].append({"role": "assistant", "content": response})
                self.display_response(response, say)
            else:
                say("I apologize, but I couldn't generate a response. Please try again.")

        @self.app.action("clear_history")
        def handle_clear_history(ack, body, say):
            ack()
            channel_id = body["channel"]["id"]
            if channel_id in self.messages:
                self.messages[channel_id] = []
                say("Conversation history has been cleared! :broom:")
            else:
                say("No conversation history to clear! :ghost:")

        @self.app.action("end_session")
        def handle_end_session(ack, body, say):
            ack()
            channel_id = body["channel"]["id"]
            try:
                self.cleanup_channel(channel_id)
                say("Session ended and chat history cleared! :wave:")
            except Exception as e:
                say("Error ending session. Please try again.")
                print(f"Error ending session: {e}")

    def format_and_chunk_response(self, response, max_length=3000):
        circle_numbers = ['①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨', '⑩']

        # Split response into main text and sources
        parts = response.split("Sources:")
        main_text = parts[0].strip()  # Access first element of the list
        sources_section = parts[1].strip() if len(parts) > 1 else ""  # Access second element if it exists

        source_lines = [line for line in sources_section.split('\n') if line.strip()]
        citation_mapping = {}
        url_order = []  # To maintain the order of URLs as they appear in sources

        # First pass: collect unique URLs and their citations, maintaining order
        for line in source_lines:
            citation_match = re.search(r'\[\^(\d+)]', line)
            url_match = re.search(r'\[(.*?)]\((https?://[^)]+)\)', line)

            if citation_match and url_match:
                citation_num = citation_match.group(1)
                title = url_match.group(1).strip()
                url = url_match.group(2).strip()

                if url not in citation_mapping:
                    # Assign a circle number based on the order of appearance
                    circle_num = circle_numbers[len(url_order)] if len(url_order) < len(
                        circle_numbers) else '•'
                    citation_mapping[url] = {
                        'emoji': circle_num,
                        'title': title,
                        'citations': [f'[^{citation_num}]']
                    }
                    url_order.append(url)  # Add URL to the order list
                else:
                    citation_mapping[url]['citations'].append(f'[^{citation_num}]')

        formatted_text = main_text
        # Replace individual citation numbers with their corresponding emojis
        # We need a way to map citation numbers back to the correct emoji based on the URL order
        citation_num_to_emoji = {}
        for url, info in citation_mapping.items():
            for citation_str in info['citations']:
                match = re.search(r'\[\^(\d+)]', citation_str)
                if match:
                    num = match.group(1)
                    # Find the emoji based on the URL's position in the ordered list
                    try:
                        url_index = url_order.index(url)
                        citation_num_to_emoji[num] = circle_numbers[url_index] if url_index < len(
                            circle_numbers) else '•'
                    except ValueError:
                        citation_num_to_emoji[num] = '•'  # Fallback if URL not found in order

        # Now replace citations in the text using the mapping
        for num, emoji in citation_num_to_emoji.items():
            formatted_text = formatted_text.replace(f'[^{num}]', f" {emoji} ")

        # Correct markdown for Slack
        # Fix bold: **text** to *text*
        formatted_text = re.sub(r'\*\*(.*?)\*\*', r'*\1*', formatted_text)
        # Fix italic: _text_ to _text_
        formatted_text = re.sub(r'_(.*?)_', r'_\1_', formatted_text)
        # Fix inline code: `text` to `text`
        formatted_text = re.sub(r'`(.*?)`', r'`\1`', formatted_text)

        # Convert markdown headings (#, ##, ### etc.) to bold text for Slack
        formatted_text = re.sub(r'^\s*#+\s*(.*)', r'\n*\1*\n', formatted_text, flags=re.MULTILINE)

        blocks = []
        current_block = ""
        in_code_block = False
        code_content = []
        code_language = ""

        for line in formatted_text.split('\n'):
            if line.strip().startswith('```'):
                if not in_code_block:
                    if current_block.strip():
                        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": current_block.strip()}})
                        current_block = ""
                    in_code_block = True
                    code_language = line.strip().replace('```', '')
                    code_content = []
                else:
                    in_code_block = False
                    code_text = '\n'.join(code_content)
                    # Correct f-string for multi-line code block in Slack.
                    # It should be ```\nlanguage\ncode\n```
                    code_block_text = f"```\n{code_language}\n{code_text}\n```"
                    blocks.append({
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": code_block_text}
                    })
            elif in_code_block:
                code_content.append(line)
            else:
                current_block += line + "\n"
                if len(current_block.strip()) > 0 and line.strip() == "":
                    blocks.append({
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": current_block.strip()}
                    })
                    current_block = ""

        if current_block.strip():
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": current_block.strip() + "\n"}
            })

        if citation_mapping:
            blocks.append({"type": "divider"})
            references_text = "*References*\n"
            # Use the url_order to ensure references appear in the same order as in sources
            for url in url_order:
                info = citation_mapping[url]
                references_text += f" {info['emoji']} <{url}|{info['title']}>\n"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": references_text.strip() + "\n"}
            })

        return blocks

    def display_response(self, response, say):
        """Display the formatted response in Slack"""
        try:
            # Format the response into blocks
            blocks = self.format_and_chunk_response(response)

            # print('blocks', blocks)

            # Send each chunk of blocks (max 50 blocks per message)
            for i in range(0, len(blocks), 50):
                chunk_blocks = blocks[i:i + 50]

                # Create fallback text from the blocks
                fallback_text = ""
                for block in chunk_blocks:
                    if block["type"] == "section":
                        fallback_text += block["text"]["text"] + "\n\n"
                    elif block["type"] == "divider":
                        fallback_text += "---\n"

                # Send message with blocks and fallback text
                say(text=fallback_text.strip(), blocks=chunk_blocks)

        except Exception as e:
            print(f"Error formatting response: {e}")
            say(text=f"I encountered an error formatting the response. Here's the plain text:\n\n{response}")

    def cleanup(self):
        """Clean up resources and clear messages when the session ends"""
        try:
            # Clear messages from all active channels
            for channel_id in self.active_channels:
                try:
                    # Get complete channel history
                    has_more = True
                    cursor = None

                    while has_more:
                        result = self.app.client.conversations_history(
                            channel=channel_id,
                            cursor=cursor,
                            limit=100
                        )

                        # Delete all messages in the conversation
                        for message in result.get('messages', []):
                            try:
                                self.app.client.chat_delete(
                                    channel=channel_id,
                                    ts=message['ts'],
                                    as_user=True
                                )
                            except Exception as e:
                                print(f"Error deleting message: {e}")

                        # Check if there are more messages
                        cursor = result.get('response_metadata', {}).get('next_cursor')
                        has_more = bool(cursor)

                except Exception as e:
                    print(f"Error cleaning channel {channel_id}: {e}")

            # Clear internal message storage
            self.messages.clear()

            # Close Snowflake connections
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()

        except Exception as e:
            print(f"Error during cleanup: {e}")

    def cleanup_channel(self, channel_id):
        """Clean up messages from a specific channel"""
        try:
            # Get complete channel history
            has_more = True
            cursor = None

            # Create user client for user message deletion
            from slack_sdk import WebClient
            user_client = WebClient(token=os.environ.get("SLACK_USER_TOKEN"))

            while has_more:
                try:
                    result = self.app.client.conversations_history(
                        channel=channel_id,
                        cursor=cursor,
                        limit=100
                    )

                    for message in result.get('messages', []):
                        try:
                            # Check if message is from bot
                            if message.get('bot_id'):
                                # Delete bot messages with bot token
                                self.app.client.chat_delete(
                                    channel=channel_id,
                                    ts=message['ts']
                                )
                            else:
                                # For user messages, only attempt deletion if user token is available
                                if os.environ.get("SLACK_USER_TOKEN"):
                                    user_client.chat_delete(
                                        channel=channel_id,
                                        ts=message['ts']
                                    )
                        except Exception as e:
                            # Log deletion errors but continue processing
                            # print(f"Could not delete message {message.get('ts')}: {e}")
                            continue

                    # Check for more messages
                    cursor = result.get('response_metadata', {}).get('next_cursor')
                    has_more = bool(cursor)

                except Exception as e:
                    print(f"Error fetching messages: {e}")
                    break

            # Clear internal message storage for this channel
            if channel_id in self.messages:
                self.messages[channel_id] = []

        except Exception as e:
            print(f"Error cleaning channel {channel_id}: {e}")
            raise

    def start(self):
        if not self.service_metadata:
            print("Error: No Cortex search services available")
            return

        handler = SocketModeHandler(self.app, os.environ["SLACK_APP_TOKEN"])
        handler.start()


if __name__ == "__main__":
    slack_bot = SlackChatBot()
    slack_bot.start()
