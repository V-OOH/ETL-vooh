import boto3
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    )

BUCKET = os.getenv('AWS_BUCKET_NAME')
PREFIXO = 'raw/'
#Essa pasta será excluída posteriormente
PASTA_LOCAL = 'data'
hoje = datetime.now().strftime('%d_%m_%Y')


# lista os arquivos do bucket
resposta = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix=PREFIXO
)

# Agora que o nome do arquivo é dinamico, fiz essa lógica que lista todos os objetos no /raw
#E guarda em um dicionário, assim eu baixo os arquivos sem me preocupar com o nome deles, apenas com a data
for arquivos in resposta.get('Contents'):

    arquivo_s3 = arquivos['Key']

    nome_arquivo = os.path.basename(arquivo_s3)

    if hoje not in nome_arquivo:
        continue

    caminho_local = os.path.join(PASTA_LOCAL, nome_arquivo)

    print(f'Baixando: {nome_arquivo}')

    s3.download_file(
        BUCKET,
        arquivo_s3,
        caminho_local
    )

print("Download concluído com sucesso!")


