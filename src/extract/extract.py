import boto3 
import os
from dotenv import load_dotenv

load_dotenv()


s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    ) 


s3.download_file('vooh-bucket','raw/dados.csv','data/dados.csv')
s3.download_file('vooh-bucket','raw/processos.csv','data/processos.csv')
print("Download concluído com sucesso!")
