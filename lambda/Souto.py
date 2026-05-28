import pandas as pd
from datetime import datetime
from datetime import timedelta
import boto3
from io import BytesIO, StringIO
import json
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

hoje = datetime.now().strftime('%d_%m_%Y')
horaAtual = datetime.now().strftime("%H:%M")
ontem = (datetime.now() - timedelta(days=1)).strftime('%d_%m_%Y')


BUCKET = 'vooh-bucket'
PREFIXO_TRUSTED = 'trusted/'

s3 = boto3.client('s3')

def lerCsv(prefixo_arquivo):
    resposta = s3.list_objects_v2(
        Bucket = BUCKET,
        Prefix = PREFIXO_TRUSTED
    )

    arquivos = []

    for item in resposta.get('Contents', []):
        arquivo_s3 = item['Key']
        nomeArquivo = os.path.basename(arquivo_s3)

        if nomeArquivo.startswith(prefixo_arquivo) and hoje in nomeArquivo:
            arquivos.append(arquivo_s3)


    listaCsv = []
    for arquivo in arquivos:
        objeto = s3.get_object(
            Bucket = BUCKET,
            Key = arquivo
        )

        conteudo = objeto['Body'].read()

        df = pd.read_csv(
            BytesIO(conteudo),
            encoding='utf-8'
        )


        listaCsv.append(df)
    return pd.concat(listaCsv, ignore_index=True)
    
#Função que vai ler o Json que está no bucket antes de incrementar os novos dados
def lerJson(idEmpresa):
    try:
        resposta = s3.get_object(
            Bucket = BUCKET,
            Key = f'client/dashIncidente_Empresa{idEmpresa}.json'
        )

        arquivo = resposta['Body'].read().decode('utf-8')
        return json.loads(arquivo)
    except:
        print("Nenhum Json encontrado...")
        return {}
    


    
def enviarAtualizacaoNoJson(idEmpresa, novoJson):
    s3.put_object(
        Bucket = BUCKET,
        Key = f'client/dashIncidente_Empresa{idEmpresa}.json',
        Body = json.dumps(novoJson, ensure_ascii=False, indent= 4),
        ContentType = 'application/json'
    )

    
csvDados = lerCsv('dados_')

conexao = mysql.connector.connect(
    host=os.getenv('DBHOST'),
    user=os.getenv('DBUSER'),
    password=os.getenv('DBPASSWORD'),
    database=os.getenv('DBNAME')
)

cursor = conexao.cursor()


cursor.execute("SELECT idDisplay, mac_addres,fkEmpresa FROM display")
resultado = cursor.fetchall()


#Conta errada, tenho que fazer a seguinte conta: tempoOperacao / tempoEsperado * 100
#disponibilidade =   (len(displaysCsv) / len(displaysBanco)) * 100
displaysCsv = set(csvDados['mac'])
dfBanco = pd.DataFrame(
    resultado,
    columns= ['idDisplays', 'mac_addres', 'fkEmpresa']
)

empresas = dfBanco['fkEmpresa'].unique()

for idEmpresa in empresas:
    displaysEmpresa = dfBanco[dfBanco['fkEmpresa'] == idEmpresa]
    displaysBancoEmpresa = set(displaysEmpresa['mac_addres'])
    displaysCsvEmpresa = displaysBancoEmpresa.intersection(displaysCsv)
    displaysOffline = displaysBancoEmpresa - displaysCsvEmpresa

    totalDisplays= len(displaysBancoEmpresa)
    quantidadeOffline = len(displaysOffline)

    novoJson = lerJson(idEmpresa)

    if ontem in novoJson:
        for hora in novoJson[ontem]:
            quantidadeOfflineOntem = novoJson[ontem][hora]["quantidadeOffline"]

    if hoje in novoJson:
        for hora in novoJson[hoje]:
            comparacaoQtdOff = quantidadeOffline - quantidadeOfflineOntem


    offlineAnterior = set()

    if len(novoJson[hoje]) > 0:
        ultimaHora = list(novoJson[hoje].keys())[-1]

        offlineAnterior = set(
            novoJson[hoje][ultimaHora]["displaysOffline"]
        )

    novosOffline = displaysOffline - offlineAnterior

    incidentesOntem = 0
    incidentesHoje = 0

    if ontem in novoJson:
        for hora in novoJson[ontem]:
            incidentesOntem += novoJson[ontem][hora].get("quantidadeNovosOffline", 0)

    if hoje in novoJson:
        for hora in novoJson[hoje]:
            incidentesHoje += novoJson[hoje][hora].get("quantidadeNovosOffline", 0)


    incidentesHoje += len(novosOffline)

    diferencaIncidentes = incidentesHoje - incidentesOntem

    registroAtual = {
        "Data": hoje,
        "Hora": horaAtual,
        "quantidadeDisplays": totalDisplays,
        "quantidadeOffline": quantidadeOffline,
        "displaysOffline": list(displaysOffline),
        "comparacaoQtdOff": comparacaoQtdOff,
        "comparacaoDiaAnterior": {
            "incidentesOntem": incidentesOntem,
            "incidentesHoje": incidentesHoje,
            "diferenca": diferencaIncidentes,
        },
            "novosOffline": len(novosOffline)
        
        #"quantidadeNovosOffline": len(novosOffline),
    }

    if hoje not in novoJson:
        novoJson[hoje] = {}

    novoJson[hoje][horaAtual] = registroAtual

    enviarAtualizacaoNoJson(idEmpresa, novoJson)


    print(f"Empresa {idEmpresa} atualizada:")
    print(json.dumps(novoJson, ensure_ascii=False, indent=4))

cursor.close()
conexao.close()
   



    


