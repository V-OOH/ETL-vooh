import os
import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

# Client S3 puxando o Bucket das variáveis de ambiente da Lambda
s3 = boto3.client('s3')
BUCKET = os.environ.get('AWS_BUCKET_NAME')

def lambda_handler(event, context):
    hoje = datetime.now().strftime('%d_%m_%Y')
    
    nome_arquivo_dados = f'dados_empresa2_{hoje}.csv'
    nome_arquivo_processos = f'processos_empresa2_{hoje}.csv'
    
    try:
        #Lendo o arwuivo de dados do S3
        print(f"Lendo: s3://{BUCKET}/trusted/{nome_arquivo_dados}")
        obj_dados = s3.get_object(Bucket=BUCKET, Key=f'trusted/{nome_arquivo_dados}')
        conteudo_dados = obj_dados['Body'].read().decode('utf-8')
        df_dados = pd.read_csv(StringIO(conteudo_dados))

        # Lendo o arwuivo de processos do S3
        print(f"Lendo: s3://{BUCKET}/trusted/{nome_arquivo_processos}")
        obj_proc = s3.get_object(Bucket=BUCKET, Key=f'trusted/{nome_arquivo_processos}')
        conteudo_proc = obj_proc['Body'].read().decode('utf-8')
        df_processos = pd.read_csv(StringIO(conteudo_proc))
        
    except s3.exceptions.NoSuchKey:
        return {
            "statusCode": 404,
            "body": json.dumps(f"Arquivos do dia {hoje} não foram encontrados no bucket.")
        }
    
    # Percorrendo todos os displays encontrados
    for id_display in df_dados["idDisplay"].unique():

        # Filtrando os dados do display atual
        df_dados_filtrado = df_dados[df_dados["idDisplay"] == id_display]
        df_processos_filtrado = df_processos[df_processos["idDisplay"] == id_display]

        # Verificando se existem dados
        if df_dados_filtrado.empty:
            continue

        # Último registro
        ultimaDados = df_dados_filtrado.iloc[-1]

        # Declaração das variáveis + conversão
        cpu_percentual = float(ultimaDados["cpu_percentual"])
        ram_percentual = float(ultimaDados["ram_percentual"])
        ram_total_gb = float(ultimaDados["ram_total_gb"])
        disco_percentual = float(ultimaDados["disco_percentual"])
        data_hora = str(ultimaDados["data_hora"])
        frequencia_atual_ghz = float(ultimaDados["frequencia_atual_ghz"])
        frequencia_max_ghz = float(ultimaDados["frequencia_max_ghz"])
        temperatura_atual = float(ultimaDados["temperatura_atual"])
        temperatura_alta = float(ultimaDados["temperatura_alta"])

        # Dashboard
        dashboard = {
            "cards": {
                "cpu": {"percentual": cpu_percentual},
                "ram": {"percentual": ram_percentual},
                "disco": {"percentual": disco_percentual}
            },
            "grafico_cpu": {
                "data_hora": data_hora,
                "frequencia_atual": frequencia_atual_ghz,
                "frequencia_max": frequencia_max_ghz,
                "temperatura_atual": temperatura_atual,
                "temperatura_alta": temperatura_alta
            },
            "grafico_ram": {
                "data_hora": data_hora,
                "consumo": ram_percentual,
                "limite": ram_total_gb
            }
        }

        # Histórico dos dados
        historico_dados = []
        for _, linha in df_dados_filtrado.iterrows():
            historico_dados.append({
                "cpu_porcentagem": float(linha["cpu_percentual"]),
                "ram_porcentagem": float(linha["ram_percentual"]),
                "disco_porcentagem": float(linha["disco_percentual"]),
                "data_hora": str(linha["data_hora"]),
                "cpu_freq": float(linha["frequencia_atual_ghz"]),
                "cpu_freq_max": float(linha["frequencia_max_ghz"]),
                "ram_total": float(linha["ram_total_gb"]),
                "temp_atual": float(linha["temperatura_atual"]),
                "temp_alta": float(linha["temperatura_alta"])
            })

        # Histórico dos processos
        historico_processos = []
        for _, linha in df_processos_filtrado.iterrows():
            historico_processos.append({
                "pid": int(linha["pid"]),
                "nomeProcesso": str(linha["nomeProcesso"]),
                "consumo_ram": float(linha["usoMemoriaProcessoMB"]),
                "consumo_cpu": float(linha["usoCpuProcesso"])
            })

        # JSON final
        json_final = {
            "idDisplay": int(id_display),
            "dashboard": dashboard,
            "historico_dados": historico_dados,
            "historico_processos": historico_processos
        }

        # Enviar para S3 
        s3.put_object(
            Bucket=BUCKET,
            Key=f"displays/{id_display}.json",
            Body=json.dumps(json_final, ensure_ascii=False, indent=4).encode('utf-8'),
            ContentType="application/json"
        )

        print(f"Display {id_display} enviado com sucesso.")

    return {
        "statusCode": 200,
        "body": json.dumps("Processamento de Hardware concluído com sucesso!")
    }