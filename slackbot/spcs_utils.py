import snowflake.connector
import os
import logging

logging.basicConfig(level=logging.INFO)


def get_login_token():
    try:
        with open('/snowflake/session/token', 'r') as f:
            return f.read()
    except FileNotFoundError:
            return None


def is_running_in_spcs():
     return get_login_token() is not None
 

def get_connection():
    configs = {
            "host": os.getenv("SNOWFLAKE_HOST"),
            "account": os.environ['SNOWFLAKE_ACCOUNT'],
            "token": get_login_token(),
            "authenticator": 'oauth',
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_USER_ROLE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA")
        }

    logging.info("*"*80)
    logging.info("Connecting to Snowflake...")
    logging.info(configs)
    logging.info("*"*80)

    conn = None
    try:
        conn = snowflake.connector.connect(**configs)
        logging.info("Connected to Snowflake using oAuth")
    except Exception as e:
        logging.error(e)
        logging.warning("Trying PAT authentication...")
        logging.info(configs)

        configs = {
            "user": os.environ['SNOWFLAKE_USER'],
            "authenticator": 'SNOWFLAKE',
            "account": os.environ['SNOWFLAKE_ACCOUNT'],
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_USER_ROLE"),
            "host": os.getenv("HOST"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "password": os.getenv("SNOWFLAKE_PASSWORD")  
        }

        conn = snowflake.connector.connect(**configs)
        logging.info("Connected to Snowflake using oAuth")

    return conn
