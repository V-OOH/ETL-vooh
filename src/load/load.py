import mysql.connector
import os
from dotenv import load_dotenv
import csv
import boto3
import json

#VERSÃO BASE DO LOAD
#Lê o .env e traz as credenciais do banco 
load_dotenv()


conexao = mysql.connector.connect(
    host=os.getenv('DBHOST'),
    user=os.getenv('DBUSER'),
    password=os.getenv('DBPASSWORD'),
    database=os.getenv('DBNAME')
)

dadosClient = []
registros = {}

#Cursor é o objeto que executa funções no banco
cursor = conexao.cursor()

with open('dadosFinais.csv', 'r', encoding="utf-8") as arquivo:
    leitura = csv.DictReader(arquivo)

    for linha in leitura:
        macAddres = linha['mac']
       
        #Percorre linha por linha e retorna o resultado desse select
        cursor.execute(
            "SELECT idDisplay, fkEmpresa from display where mac_addres = %s",(macAddres,)
        )

        #Traz o resultado e guarda na variável
        resultado = cursor.fetchone()

        if resultado is not None:
            idDisplay = resultado[0]
            fkEmpresa = resultado[0]

            chave = (idDisplay, linha["data_hora"])
            if chave not in registros:

            #Monta o dicionário com o formato que queremos
                registros[chave] = {
                "idDisplay": idDisplay,
                "idEmpresa": fkEmpresa,
                "macAddres": macAddres,
                "dataHora": linha["data_hora"],
                "cpu%": float(linha["cpu_percentual"]),
                "ram%": float(linha["ram_percentual"]),
                "ramTotal": float(linha["ram_total_gb"]),
                "ramUsada": float(linha["ram_total_gb"]) - float(linha["ram_disponivel_gb"]),
                "disco%": float(linha["disco_percentual"]),
                "total_disco_gb": float(linha["total_disco_gb"]),
                "disco_usado_gb": float(linha["disco_usado_gb"]),
                "upload_mb":float(linha["upload_mb"]),
                "download_mb":float(linha["download_mb"]),
                "top5processos": []

            }

            processo = {
            "pid": int(linha["pid"]),
            "usuario": linha["usuario"],
            "nomeProcesso": linha["nomeProcesso"],
            "usoMemoriaProcesso": float(linha["usoMemoriaProcessoMB"]),
            "usoCpuProcesso": float(linha["usoCpuProcesso"])
        }

            registros[chave]["top5processos"].append(processo)

        else:
            print(f"MAC não encontrado no banco: {macAddres}")

dadosClient = list(registros.values())
print(linha["nomeProcesso"])

#Finaliza a Query 
cursor.close()
conexao.close()

#Crio o .json com os dados coletados
with open('dados_dashboard.json', "w", encoding="utf-8") as arquivoJson:
    json.dump(dadosClient, arquivoJson,indent=4, ensure_ascii=False)

#s3 = boto3.client('s3', 
   # aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
   # aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    #aws_session_token=os.getenv('AWS_SESSION_TOKEN')
#)

#s3.upload_file('dados_dashboard.json','vooh-bucket','client/dados_dashboard.json')
print("Upload feito com sucesso!")