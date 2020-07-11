# AML insurance csv loader

The MySQL and ElasticSearch loader for Cathay's AML insurance CSV

## Set up environment

1.  Install Python3.7+
2.  cmd > `pip3 install -r requirements.txt`
3.  change .env file to set the environment for ElasticSearch and MySQL connection
4.  cmd > `source .env`

## Load the CSV files to ElasticSearch and MySQL

1.  cmd > `python3 aml_loader.py`
