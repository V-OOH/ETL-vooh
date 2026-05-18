import mysql.connector
import os
from dotenv import load_dotenv
import pandas as pd
import json
from datetime import datetime
import boto3

load_dotenv()

BUCKET = os.getenv('AWS_BUCKET_NAME')

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
)

hoje = datetime.now().strftime('%d_%m_%Y')

conexao = mysql.connector.connect(
    host=os.getenv('DBHOST'),
    user=os.getenv('DBUSER'),
    password=os.getenv('DBPASSWORD'),
    database=os.getenv('DBNAME')
)

cursor = conexao.cursor()

dadosMaquinas = f'dados_tratados_{hoje}.csv'
dadosProcessos = f'processos_tratados_{hoje}.csv'

# Lê os CSVs tratados e armazena na variável
dados = pd.read_csv(dadosMaquinas, encoding='utf-8')
processos = pd.read_csv(dadosProcessos, encoding='utf-8')

# Cria o top 5 processos que mais consomem memória RAM (segui o padrão do gerenciador de tarefas)
#Sort_values = ordena do MAIOR uso de RAM para o MENOR
#groupby = Como ele trata todos os processos do arquivo ele já guarda de uma vez só o top 5 de cada máquina usando como parametro o macAdress, ip e dataHora
#head(5) = guarde os 5 primeiros de cada maquina
processos_top5 = (
    processos.sort_values('usoMemoriaProcessoMB', ascending=False).groupby(['mac', 'ip', 'data_hora']).head(5)
)

# Junta dados da máquina com os processos
arquivoFinal = pd.merge(
    dados,
    processos_top5,
    on=['mac', 'ip', 'data_hora'],
    how='inner'
)

#Criar um json por empresa por motivos de segurança
empresas = {}

#A função iterrow retorna um indice e uma linha (recomendo pesquisar se quiser entender esse For)
#Mas como não queremos trabalhar com o indice de cada linha, usei o _, pra pular o primeiro valor
#E usar só o objeto retornado (a linha no caso)
for _, linha in arquivoFinal.iterrows():
    macAddres = linha['mac']

    cursor.execute(
        "SELECT idDisplay, fkEmpresa FROM display WHERE mac_addres = %s",
        (macAddres,)
    )

    resultado = cursor.fetchone()

    if resultado is None:
        print(f"MAC não encontrado no banco: {macAddres}")
        continue

    idDisplay = resultado[0]
    fkEmpresa = str(resultado[1])

    display_minuto = (idDisplay, linha['data_hora'])

    if fkEmpresa not in empresas:
        empresas[fkEmpresa] = {}

    if display_minuto not in empresas[fkEmpresa]:
        empresas[fkEmpresa][display_minuto] = {
            "idDisplay": int(idDisplay),
            "idEmpresa": int(fkEmpresa),
            "macAddres": macAddres,
            "dataHora": linha["data_hora"],

            "cpu%": float(linha["cpu_percentual"]),
            "ram%": float(linha["ram_percentual"]),
            "ramTotal": float(linha["ram_total_gb"]),
            "ramUsada": round(float(linha["ram_total_gb"]) - float(linha["ram_disponivel_gb"]),2
            ),

            "disco%": float(linha["disco_percentual"]),
            "total_disco_gb": float(linha["total_disco_gb"]),
            "disco_usado_gb": float(linha["disco_usado_gb"]),

            "upload_mb": float(linha["upload_mb"]),
            "download_mb": float(linha["download_mb"]),

            "top5processos": []
        }

    processo = {
        "pid": int(linha["pid"]),
        "usuario": linha["usuario"],
        "nomeProcesso": linha["nomeProcesso"],
        "usoMemoriaProcesso": float(linha["usoMemoriaProcessoMB"]),
        "usoCpuProcesso": float(linha["usoCpuProcesso"])
    }

    empresas[fkEmpresa][display_minuto]["top5processos"].append(processo)


cursor.close()
conexao.close()


# Cria um JSON separado para cada empresa
for idEmpresa, registros in empresas.items():
    dados = list(registros.values())

    nomeArquivo = f'dados_dashboard_empresa_{idEmpresa}_{hoje}.json'

    with open(nomeArquivo, 'w', encoding='utf-8') as arquivoJson:
        json.dump(
            dados,
            arquivoJson,
            indent=4,
            ensure_ascii=False
        )

        
    s3.upload_file(
        nomeArquivo,
        BUCKET,
        f'client/{nomeArquivo}'
    )
    print(f"JSON criado: {nomeArquivo}")