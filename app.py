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
    "claude-3-5-sonnet",
    "claude-3-7-sonnet",
]

load_dotenv()


class SlackChatBot:
    def __init__(self):
        # Initialize Snowflake connection using JWT

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

        # self.conn = snowflake.connector.connect(
        #     account=os.environ['SNOWFLAKE_ACCOUNT'],
        #     user=os.environ['SNOWFLAKE_USER'],
        #     private_key_path=get_private_key(),
        #     database=os.environ['SNOWFLAKE_DATABASE'],
        #     schema=os.environ['SNOWFLAKE_SCHEMA']
        # )
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
        self.cursor.execute("SHOW CORTEX SEARCH SERVICES IN DATABASE")
        services = self.cursor.fetchall()
        service_metadata = []
        if services:
            for s in services:
                svc_name = s[1]  # name column in show services
                self.cursor.execute(f"DESC CORTEX SEARCH SERVICE {svc_name}")
                svc_search_col = self.cursor.fetchone()[3]  # search_column in desc
                svc_search_col = 'content'
                service_metadata.append(
                    {"name": svc_name, "search_column": svc_search_col}
                )
        return service_metadata


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
            columns=[search_col, "URL"],
            limit=k * 2,  # Get more results for better combination
            strategy='semantic'
        ).results

        # Perform keyword search with scoring
        keyword_results = service.search(
            query,
            columns=[search_col, "URL"],
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
        """Query the Cortex search service and format results"""
        db = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")

        service = (self.root.databases[db]
        .schemas[schema]
        .cortex_search_services[self.selected_service])

        results = self.hybrid_search(query, service, k=self.num_retrieved_chunks)

        search_col = [s["search_column"] for s in self.service_metadata
                      if s["name"] == self.selected_service][0]

        context_str = ""
        for i, r in enumerate(results):
            url = r.get("URL", "").strip() or "#"
            context_str += f"Context document {i + 1}: {r[search_col].strip()} [URL: {url}]\n\n"

        if self.debug:
            print("Context documents:", context_str)

        return context_str

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

            context = self.query_cortex_search_service(question)

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
            <question>
            {question}
            </question>
            [/INST]
            """

            if channel_id not in self.messages:
                self.messages[channel_id] = []

            context = self.query_cortex_search_service(question)
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
        """
        Format and split large responses into smaller chunks for Slack with proper indentation
        """
        # Replace citation numbers with emojis
        emoji_citations = {
            '[^1]': ':one:', '[^2]': ':two:', '[^3]': ':three:',
            '[^4]': ':four:', '[^5]': ':five:', '[^6]': ':six:',
            '[^7]': ':seven:', '[^8]': ':eight:', '[^9]': ':nine:',
            '[^10]': ':keycap_ten:'
        }

        # Split response into main text and sources
        parts = response.split("Sources:")
        main_text = parts[0].strip()
        sources = parts[1].strip() if len(parts) > 1 else ""

        # Initialize blocks list
        all_blocks = []

        # Replace citation numbers with emojis in main text
        for citation, emoji in emoji_citations.items():
            main_text = main_text.replace(citation, emoji)

        # Process main text with proper indentation
        text_chunks = []
        current_chunk = ""

        # Add indentation for paragraphs and lists
        paragraphs = main_text.split('\n\n')
        for paragraph in paragraphs:
            # Add indentation for bullet points
            if paragraph.strip().startswith('•'):
                formatted_paragraph = paragraph.replace('•', '   •')
            # Add indentation for numbered lists
            elif re.match(r'^\d+\.', paragraph.strip()):
                formatted_paragraph = '   ' + paragraph
            else:
                formatted_paragraph = paragraph

            if len(current_chunk + formatted_paragraph) > max_length:
                if current_chunk:
                    text_chunks.append(current_chunk.strip())
                current_chunk = formatted_paragraph + '\n\n'
            else:
                current_chunk += formatted_paragraph + '\n\n'

        if current_chunk:
            text_chunks.append(current_chunk.strip())

        # Add text blocks with proper formatting
        for chunk in text_chunks:
            all_blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": chunk
                }
            })

        # Handle sources if present
        if sources:
            all_blocks.append({"type": "divider"})

            # Process each source line
            source_lines = sources.split('\n')
            formatted_sources = []
            seen_urls = set()

            for line in source_lines:
                citation_match = re.search(r'\[\^(\d+)\]', line)
                url_match = re.search(r'\((.*?)\)', line)

                if citation_match and url_match:
                    citation_num = citation_match.group(1)
                    url = url_match.group(1)

                    if url not in seen_urls:
                        emoji = emoji_citations.get(f'[^{citation_num}]', ':question:')
                        formatted_sources.append(f'   {emoji} <{url}>')
                        seen_urls.add(url)

            if formatted_sources:
                sources_text = "*Sources:*\n" + "\n".join(formatted_sources)
                all_blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": sources_text
                    }
                })

        return all_blocks

    def display_response(self, response, say):
        """
        Format and display the response in Slack with size handling and fallback text
        """
        try:
            # Format and chunk the response
            blocks = self.format_and_chunk_response(response)

            # Send each chunk of blocks (max 50 blocks per message)
            for i in range(0, len(blocks), 50):
                chunk_blocks = blocks[i:i + 50]
                # Add fallback text from the blocks
                fallback_text = "\n".join(
                    block["text"]["text"]
                    for block in chunk_blocks
                    if block["type"] == "section"
                )
                say(text=fallback_text, blocks=chunk_blocks)

        except Exception as e:
            # Fallback to simple text message if formatting fails
            say(text="I encountered an error formatting the response. Here's the plain text:\n\n" + response)

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

            # Create user client once outside the loop
            from slack_sdk import WebClient
            user_client = WebClient(token=os.environ.get("SLACK_USER_TOKEN"))

            while has_more:
                try:
                    result = self.app.client.conversations_history(
                        channel=channel_id,
                        cursor=cursor,
                        limit=100
                    )

                    # Delete all messages in the conversation
                    for message in result.get('messages', []):
                        try:
                            # Try with bot token first
                            try:
                                self.app.client.chat_delete(
                                    channel=channel_id,
                                    ts=message['ts']
                                )
                            except:
                                # If bot deletion fails, try with user token
                                user_client.chat_delete(
                                    channel=channel_id,
                                    ts=message['ts']
                                )
                        except Exception as e:
                            print(f"Error deleting message {message.get('ts')}: {e}")

                    # Check if there are more messages
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
