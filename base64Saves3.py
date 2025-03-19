import json
import urllib.parse
import boto3
import base64
from io import BytesIO
print('Loading function')
bucket = 's3-my-backet'
s3 = boto3.client('s3')

def lambdaEncode(encoded_binary,file_name):
    # TODO implement
    decoded_binary = base64.b64decode(encoded_binary)
    excel_buffer = BytesIO(decoded_binary)
        
    # Upload to S3
    # Seek to the beginning of the stream
    excel_buffer.seek(0)
    print ("excel_buffer.seek(0) passed")
    # Upload the file
    s3.upload_fileobj(excel_buffer, bucket, file_name)

    print("# Put an object in S3")
    # Put an object in S3    
    return {
        'statusCode': 200,
        'body': json.dumps('Put object in S3 from Lambda!=>'+ file_name)
    }


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    #bucket = event['Records'][0]['s3']['bucket']['name']
    encoded_binary=event['encoded_binary']
    key =event['key']
    lambdaresponse=lambdaEncode(encoded_binary,key)
    print (json.dumps(lambdaresponse))
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        object_body = 'This is test content.' 
        s3.put_object(Bucket=bucket, Key=key, Body=object_body)


        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
