import os

# DB CONNECTION
HOST = os.environ['pg_host']
PORT = os.environ['pg_port']
DB_NAME = os.environ['pg_dbname']
USERNAME = os.environ['pg_username']
PASSWORD = os.environ['pg_password']

DB_CONN_STRING = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"

# API GATEWAY PROXY INTEGRATION
API_GATEWAY_PROXY_INTEGRATION = os.environ.get("AWS_LAMBDA_API_GATEWAY_PROXY_INTEGRATION", "True") == "True"
