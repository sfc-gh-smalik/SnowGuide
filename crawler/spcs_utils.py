import os
from snowflake.snowpark import Session

def is_running_in_spcs():
    return os.path.exists("/snowflake/session/token")

def get_login_token():
    """
    Read the login token supplied automatically by Snowflake. These tokens
    are short lived and should always be read right before creating any new connection.
    """
    with open("/snowflake/session/token", "r") as f:
        return f.read()


def get_connection_params():
    """
    Construct Snowflake connection params from environment variables.
    """
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

    # Custom environment variables
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    
    if os.path.exists("/snowflake/session/token"):
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "authenticator": "oauth",
            "token": get_login_token(),
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }
    else:
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "user": SNOWFLAKE_USER,
            "password": SNOWFLAKE_PASSWORD,
            "role": SNOWFLAKE_ROLE,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }
    
def get_connection():
    return Session.builder.configs(get_connection_params()).create()