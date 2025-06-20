import re
import snowflake.connector
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from snowflake.core import Root
import atexit
import logging
from spcs_utils import get_connection
from openai import OpenAI


logging.basicConfig(level=os.getenv('LOGLEVEL', 'INFO').upper())

CORTEX_LLM = "cortex"
OPENAI_LLM = "openai"
OPENAI_MODEL_NAME = "gpt-4.1"

MODELS = [
    "claude-3-5-sonnet",
    "claude-3-7-sonnet",
]

load_dotenv()


class SlackChatBot:
    def __init__(self):
        # Initialize Snowflake connection
        self.conn = None
        self.conn = self._get_connection()
        #self.connection_time = datetime.now() + timedelta(minutes=15) # Refresh every 15 minutes
        self.cursor = self.conn.cursor()

        self.messages = {}  # Dict to store messages per channel
        self.num_retrieved_chunks = 10
        self.num_chat_messages = 10
        self.use_chat_history = True
        self.debug = True if (os.getenv('LOGLEVEL', 'INFO').upper() == 'DEBUG') else False
      
        # Search-Related properties
        self.openai_api_key = os.getenv('OPENAI_API_KEY', None)
        self.llm_api = os.getenv('LLM_API', OPENAI_LLM)
        self.model_name = OPENAI_MODEL_NAME if (self.llm_api == OPENAI_LLM) else MODELS[0]

        self.cortex_search_database = os.getenv('CORTEX_SEARCH_DATABASE', 'SNOWFLAKE_DOCUMENTATION')
        self.cortex_search_schema = os.getenv('CORTEX_SEARCH_SCHEMA', 'SHARED')
        self.cortex_search_service = os.getenv('CORTEX_SEARCH_SERVICE', 'CKE_SNOWFLAKE_DOCS_SERVICE')
        self.cortex_search_column = os.getenv('CORTEX_SEARCH_COLUMN', 'chunk')
        self.cortex_url_column = os.getenv('CORTEX_URL_COLUMN', 'source_url')
        self.selected_service = self.cortex_search_service

        self.openai_client = None
        if (self.llm_api == OPENAI_LLM):
            self.openai_client = OpenAI()
            self.model_name = OPENAI_MODEL_NAME

        # Initialize Slack app
        self.app = App(token=os.environ["SLACK_BOT_TOKEN"])

        # Send welcome message when bot joins a channel
        @self.app.event("member_joined_channel")
        def handle_member_joined_channel(event, say):
            if event.get("user") == self.app.client.auth_test()["user_id"]:
                self.send_welcome_message(event["channel"], say)

        self.setup_slack_handlers()
        self.root = Root(self._get_connection())
        # Store channel IDs for cleanup
        self.active_channels = set()

        # Register cleanup method
        atexit.register(self.cleanup)

        logging.info("*" * 80 + "\nSlackChatBot instance initialized with")
        for key, value in vars(self).items():
            logging.info(f"{key}: {value}")
        logging.info("*" * 80)

    # Refresh the connection every 15 minutes
    def _get_connection(self):
        current_datetime = datetime.now()
        if (self.conn is None or current_datetime >= self.connection_time):
            self.conn = get_connection()
            self.connection_time = current_datetime + timedelta(minutes=15)
        
        return self.conn


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
        search_col = self.cortex_search_column

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

    def query_cortex_search_service(self, query):

        cortex_search_service = (
            self.root.databases[self.cortex_search_database]
            .schemas[self.cortex_search_schema]
            .cortex_search_services[self.cortex_search_service]
        )

        context_documents = cortex_search_service.search(
            query,
            # TODO: Parameterize
            columns=["chunk", "document_title", "source_url"],
            limit=self.num_retrieved_chunks
        )
        results = context_documents.results

        search_col = self.cortex_search_column 
        context_str = ""
        citations = []
        seen_urls = set()  # Track unique URLs

        for i, r in enumerate(results):
            content = None
            # TODO: Parameterize
            for col_name in [search_col, "chunk", "CHUNK", "content", "CONTENT"]:
                if col_name in r:
                    content = r[col_name]
                    break

            if content is None:
                if self.debug:
                    logging.warning(f"Could not find content in result {i + 1}. Available keys: {list(r.keys())}")
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
        result = None

        logging.info(f"Answering request using {self.llm_api} API and model {self.model_name}")
        logging.debug(f"{prompt=}")

        cursor = self._get_connection().cursor()
        if (self.llm_api == CORTEX_LLM):
            cursor.execute(
                "SELECT snowflake.cortex.complete(%s, %s)",
                (self.model_name, prompt)
            )
            result = cursor.fetchone()
            result = result[0] if result else None
        elif (self.llm_api == OPENAI_LLM):
            response = self.openai_client.responses.create(
                model=self.model_name,
                input=prompt
                )
            result = response.output_text
        else:
            raise Exception(f"Unknown LLM API {self.llm_api}")

        logging.debug(f"{result=}")
        return result

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

            #### Extra?context, citations = self.query_cortex_search_service(question)
            response = self.complete(prompt)

            # Delete thinking message
            try:
                self.app.client.chat_delete(
                    channel=channel_id,
                    ts=thinking_response['ts']
                )
            except Exception as e:
                logging.error(f"Error deleting thinking message: {e}")

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
                logging.error(f"Error ending session: {e}")

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

            # logging.info('blocks', blocks)

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
            logging.info(f"Error formatting response: {e}")
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
                                logging.info(f"Error deleting message: {e}")

                        # Check if there are more messages
                        cursor = result.get('response_metadata', {}).get('next_cursor')
                        has_more = bool(cursor)

                except Exception as e:
                    logging.info(f"Error cleaning channel {channel_id}: {e}")

            # Clear internal message storage
            self.messages.clear()

            # Close Snowflake connections
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()

        except Exception as e:
            logging.info(f"Error during cleanup: {e}")

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
                            # logging.info(f"Could not delete message {message.get('ts')}: {e}")
                            continue

                    # Check for more messages
                    cursor = result.get('response_metadata', {}).get('next_cursor')
                    has_more = bool(cursor)

                except Exception as e:
                    logging.info(f"Error fetching messages: {e}")
                    break

            # Clear internal message storage for this channel
            if channel_id in self.messages:
                self.messages[channel_id] = []

        except Exception as e:
            logging.info(f"Error cleaning channel {channel_id}: {e}")
            raise

    def start(self):
        handler = SocketModeHandler(self.app, os.environ["SLACK_APP_TOKEN"])
        handler.start()


if __name__ == "__main__":
    slack_bot = SlackChatBot()
    logging.info("Starting SlackChatBot")
    slack_bot.start()
