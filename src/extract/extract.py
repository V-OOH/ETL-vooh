import boto3 
import os
from dotenv import load_dotenv

load_dotenv()


s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    ) 

bucket_name = 'vooh-bucket'
s3_file_key = 'raw/dados.csv'
local_file_path = 'src/dados.csv'

s3.download_file(bucket_name, s3_file_key, local_file_path)
print("Download concluído com sucesso!")
