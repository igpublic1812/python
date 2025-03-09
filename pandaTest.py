import json
import pandas as pd
import boto3
import psycopg2
import os
from io import BytesIO

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
    dml_statements =[
       "select to_json(Y) from (SELECT array_agg(address1) AS address1,array_agg(address2) as address2, array_agg(zip) as zip,array_agg(email_contact) as email_contact FROM bldprop.building) Y;"
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
        for dml in dml_statements:
            print(f"try execute ddl : {dml}")
            cursor.execute(dml)
        data_json = cursor.fetchone()
        print(f" data_json: {data_json}") 
        # Create DataFrame
        df = pd.DataFrame(data_json)
        print(f" df: {df}")    
        # Convert DataFrame to JSON
        json_data = df.to_json(orient='records')        
        # Write DataFrame to Excel in memory
        #excel_buffer = BytesIO()
        print("start pd.ExcelWriter excel_buffer, openpyxl")        
        # Write the DataFrame to the buffer using pd.ExcelWriter
        #with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        #    df.to_excel(writer, sheet_name='Sheet1', index=False)
        
        print("finish pd.ExcelWriter ")
        # Upload to S3
        s3_client = boto3.client('s3')
        bucket_name = 's3-my-backet'
        file_name = 'BLD-RPT.xlsx'

        # Seek to the beginning of the stream
        #excel_buffer.seek(0)

        # Upload the file
        #s3_client.upload_fileobj(excel_buffer, bucket_name, file_name)
        bucket_name = 's3-my-backet'
        key = file_name
        print("# Put an object in S3")
        # Put an object in S3
        s3_client.put_object(Bucket=bucket_name, Key=key, Body='Hello from Lambda!')
    
        # Get the object from S3
        #response = s3.get_object(Bucket=bucket_name, Key=key)
        #print(response['Body'].read().decode('utf-8'))



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
