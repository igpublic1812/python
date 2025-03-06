import json
import pandas as pd
import boto3
import psycopg2
import os
from dotenv import load_dotenv

def lambda_handler(event, context):
    load_dotenv()
    db_host = event['db_host']
    db_port = event['db_port']
    db_name = event['db_name']
    db_user = event['db_user']
    db_password = event['db_password']
    print(f"Database db_host: {db_host}")
    print(f"Secret db_user: {db_user}")
    print(f"db_name : {db_name}")
    connection=None
    cursor=None
    ddl_statements = [
        "CREATE DATABASE dbbld;",
        "CREATE SCHEMA bldprop;",
        "CREATE USER dbbld_admin WITH PASSWORD 'xxxxx';",
        "grant all privileges on database dbbld to dbbld_admin;",
        "grant all privileges on all tables in schema bldprop  to dbbld_admin;",
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bldprop GRANT ALL ON TABLES TO dbbld_admin;",
    ]
    try:
        print(f"try connect to db_name : {db_name}")
        # Establish connection
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        connection.autocommit = True
        cursor = connection.cursor()
        for ddl in ddl_statements:
            print(f"try execute ddl : {ddl}")
            cursor.execute(ddl)
        # Example query
        cursor.execute("SELECT version();")
        print(f"DB connected: {connection}")
    except psycopg2.Error as e:
        print(f"DB Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("Connection closed.")

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
