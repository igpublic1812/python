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
    ddl_statements =[
        "CREATE TABLE IF NOT EXISTS bldprop.building(id numeric NOT NULL,address1 character varying COLLATE pg_catalog.\"default\",address2 character varying COLLATE pg_catalog.\"default\",zip character varying COLLATE pg_catalog.\"default\",email_contact character varying COLLATE pg_catalog.\"default\",CONSTRAINT building_pkey PRIMARY KEY (id)) TABLESPACE pg_default;",
        "ALTER TABLE IF EXISTS bldprop.building OWNER to postgres;",
        "GRANT ALL ON TABLE bldprop.building TO dbbld_admin;",
        "GRANT ALL ON TABLE bldprop.building TO postgres;"
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
