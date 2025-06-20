import snowflake.connector
import os
import logging
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO').upper())


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
        logging.warning(e)
        logging.warning("Could not connect with oAuth, Trying PAT authentication...")
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
