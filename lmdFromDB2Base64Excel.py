import json
import psycopg2
import pandas as pd
from io import BytesIO
import base64
base64data=None
def toexcelBase64(data_json):
    # Create DataFrame
    # data_json = event["data_json"]
    print(f" data_json final: {data_json}")
   
    df = pd.DataFrame(data_json)
    print(f" dataframe: {df}")    
    # Convert DataFrame to JSON
    json_data = df.to_json(orient='records')        
    # Write DataFrame to Excel in memory
    excel_buffer = BytesIO()
    print("start pd.ExcelWriter excel_buffer, openpyxl")        
    # Write the DataFrame to the buffer using pd.ExcelWriter
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
    print("end pd.ExcelWriter excel_buffer, openpyxl")        
    binary_data = excel_buffer.getvalue()
    encoded_data = base64.b64encode(binary_data).decode('utf-8')
    return encoded_data
def lambda_handler(event, context):
    db_host = event['db_host']
    db_port = event['db_port']
    db_name = event['db_name']
    db_user = event['db_user']
    db_sql = event['db_sql']
    db_password = event['db_password']
    print(f"Database db_host: {db_host}")
    print(f"Secret db_user: {db_user}")
    print(f"db_name : {db_name}")
    print(f"db_sql : {db_sql}")
    connection=None
    cursor=None
    dml_statements =[
       db_sql 
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
            print(f"try execute ddl or dml: {dml}")
            cursor.execute(dml)
            data_json = cursor.fetchone()
            print(f" data_json: {data_json}")
            base64data=toexcelBase64(data_json)
            print(f" base64data: {base64data}") 
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
        'data_json': data_json,
        'base64data': base64data
    }
