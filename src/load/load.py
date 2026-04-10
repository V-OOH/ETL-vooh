import mysql.connector
import os
from dotenv import load_dotenv
import csv
import boto3
import json

#Lê o .env e traz as credenciais do banco 
load_dotenv()


conexao = mysql.connector.connect(
    host=os.getenv('DBHOST'),
    user=os.getenv('DBUSER'),
    password=os.getenv('DBPASSWORD'),
    database=os.getenv('DBNAME')
)

dadosClient = []

#Cursor é o objeto que executa funções no banco
cursor = conexao.cursor()

with open('dadosTratados.csv', 'r', encoding="utf-8") as arquivo:
    leitura = csv.DictReader(arquivo)

    for linha in leitura:
        macAddres = linha['mac']
        
        #Percorre linha por linha e retorna o resultado desse select
        cursor.execute(
            "SELECT idDisplay from display where mac_addres = %s",(macAddres,)
        )

        #Traz o resultado e guarda na variável
        resultado = cursor.fetchone()

        if resultado is not None:
            idDisplay = resultado[0]

            #Monta o dicionário com o formato que queremos
            registro = {
                "idDisplay": idDisplay,
                "macAddres": macAddres,
                "cpu": float(linha["cpu_percentual"]),
                "ram": float(linha["ram_percentual"]),
                "disco": float(linha["disco_percentual"]),
                "dataHora": linha["data_hora"]

            }

            dadosClient.append(registro)

        else:
            print(f"MAC não encontrado no banco: {mac}")

#Finaliza a Query 
cursor.close()
conexao.close()

#Crio o .json com os dados coletados
with open('dados_dashboard.json', "w", encoding="utf-8") as arquivoJson:
    json.dump(dadosClient, arquivoJson,indent=4, ensure_ascii=False)

s3 = boto3.client('s3', 
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
)

s3.upload_file('dados_dashboard.json','vooh-bucket','client/dados_dashboard.json')
print("Upload feito com sucesso!")