import pandas as pd
import glob
from datetime import datetime
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

hoje = datetime.now().strftime('%d_%m_%Y')

#O script de captura salva os processos e os dados da máquinas em segundos diferentes, e isso retorna um erro na 
#hora de juntar os dois csv's lá na frente. Então trunquei para que valide somente a hora e minuto
def truncar_minuto(coluna):
    return pd.to_datetime(
        coluna,
        format='%d-%m-%Y %H:%M:%S'
    ).dt.strftime('%d-%m-%Y %H:%M')


#Utilizei a biblioteca Glob que verifica todos os dados em uma determinada pasta que possuem um nome padrão
#e depois faz a leitura de cada arquivo csv e guarda na variável
#Fiz uma tratativa de erro pra ficar mais profissional

try:

    dadosMaquinas = glob.glob(f'data/dados_{hoje}_*.csv')

    if not dadosMaquinas:
        raise FileNotFoundError(
            f'Nenhum arquivo de dados de Máquina encontrado para {hoje}'
        )

    csvDados = pd.concat(
        [pd.read_csv(arq, encoding='utf-8')
        for arq in dadosMaquinas],
        ignore_index=True
    )

    dadosProcessos = glob.glob(f'data/processos_{hoje}_*.csv')

    if not dadosProcessos:
        raise FileNotFoundError(
            f'Nenhum arquivo de processos encontrado para {hoje}'
        )

    csvProcessos = pd.concat(
        [pd.read_csv(arq, encoding='utf-8') 
        for arq in dadosProcessos],
        ignore_index=True
    )

    



#Diferente da biblioteca CSV, o Pandas faz o tratamento por colunas, evitando a necessidade de uso do For
    dados = pd.DataFrame({
        'data_hora': truncar_minuto(csvDados['data_hora']),

     #Converter dados de disco
        'total_disco_gb': (csvDados['total_disco'].astype(int) / (1024 ** 3)).round(2),
        'disco_usado_gb': (csvDados['disco_usado'].astype(int) / (1024 ** 3)).round(2),
        'disco_livre_gb': (csvDados['disco_livre'].astype(int) / (1024 ** 3)).round(2),
        'disco_percentual': csvDados['disco_percentual'].astype(float),

    #Converter dados de CPU
        'processador_nome': csvDados['processador_nome'],
        'nucleos_fiscos': csvDados['nucleos_fiscos'],
        'nucleos_totais': csvDados['nucleos_totais'],
        'frequencia_max_ghz': (csvDados['frequencia_max'].astype(float) / 1000).round(2),
        'frequencia_atual_ghz': (csvDados['frequencia_atual'].astype(float) / 1000).round(2),
        'cpu_percentual': csvDados['cpu_percentual'].astype(float),

    #Converter dados de RAM
        'ram_total_gb': (csvDados['ram_total'].astype(float) / (1024 ** 3)).round(2),
        'ram_disponivel_gb': (csvDados['ram_disponivel'].astype(float) / (1024 ** 3)).round(2),
        'ram_percentual': csvDados['ram_percentual'].astype(float),

    #Converter os dados de rede
        'upload_mb': (csvDados['upload'].astype(int) / (1024 ** 2)).round(2),
        'download_mb': (csvDados['download'].astype(int) / (1024 ** 2)).round(2),

        'mac': csvDados['mac'],
        'ip': csvDados['ip']
    })


#Tratamento dos processos
    processos = pd.DataFrame({
        'data_hora': truncar_minuto(csvProcessos['data_hora']),
        'pid': csvProcessos['pid'],
        'usuario': csvProcessos['usuario'],
        'nomeProcesso': csvProcessos['nome'],
        'usoMemoriaProcessoMB': (csvProcessos['memoria'].astype(int) / (1024 ** 2)).round(2),
        'usoCpuProcesso': csvProcessos['uso_cpu'],
        'mac': csvProcessos['mac'],
        'ip': csvProcessos['ip']
    })



    dados.to_csv(
        f'dados_tratados_{hoje}.csv',
        index=False,
        encoding='utf-8'
    )

    processos.to_csv(
        f'processos_tratados_{hoje}.csv',
        index=False,
            encoding='utf-8'
    )

    arquivoDados = f'dados_tratados_{hoje}.csv'
    arquivoProcessos = f'processos_tratados_{hoje}.csv'

    s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    )

    BUCKET = os.getenv('AWS_BUCKET_NAME')

    s3.upload_file(
        arquivoDados,
        BUCKET,
        f'trusted/{arquivoDados}'
    )

    s3.upload_file(
        arquivoProcessos,
        BUCKET,
        f'trusted/{arquivoProcessos}'
    )

    print("Tratamento de dados finalizado com sucesso!")

    
except FileNotFoundError as erro:
    print(erro)

    
