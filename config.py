import os

# ElasicSearch environment variables
ES_IP = os.getenv("ES_IP")
ES_PORT = os.getenv("ES_PORT")
ES_URL = ES_IP + ":" + ES_PORT

# MySQL environment variables
MYSQL_IP = os.getenv("MYSQL_IP")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
