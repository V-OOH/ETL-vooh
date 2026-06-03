import pandas as pd
import json
import boto3

# Carregar arquivos
df_dados = pd.read_csv(
    'src/transform/dados_empresa2_02_06_2026.csv'
)

df_processos = pd.read_csv(
    'src/transform/processos_empresa2_02_06_2026.csv'
)

# colunas = ['data_hora', 'frequencia_max_ghz','frequencia_atual_ghz',
# 'cpu_percentual', 'ram_total_gb','ram_disponivel_gb', 'ram_percentual',
# 'disco_percentual','temperatura_atual','temperatura_alta']

# print(df_dados[colunas])

# colunas = ['pid', 'nomeProcesso', 'usoMemoriaProcessoMB','usoCpuProcesso']
# print(df_processos[colunas])

s3 = boto3.client("s3")

# Percorrendo todos os displays encontrados
for id_display in df_dados["idDisplay"].unique():

    # Filtrando os dados do display atual
    df_dados_filtrado = df_dados[
        df_dados["idDisplay"] == id_display
    ]

    df_processos_filtrado = df_processos[
        df_processos["idDisplay"] == id_display
    ]

    # Verificando se existem dados
    if df_dados_filtrado.empty:
        continue

    # Último registro
    ultimaDados = df_dados_filtrado.iloc[-1]

    # Declaração das variáveis
    cpu_percentual = ultimaDados["cpu_percentual"]

    ram_percentual = ultimaDados["ram_percentual"]
    ram_total_gb = ultimaDados["ram_total_gb"]

    disco_percentual = ultimaDados["disco_percentual"]

    data_hora = ultimaDados["data_hora"]

    frequencia_atual_ghz = ultimaDados["frequencia_atual_ghz"]
    frequencia_max_ghz = ultimaDados["frequencia_max_ghz"]

    temperatura_atual = ultimaDados["temperatura_atual"]
    temperatura_alta = ultimaDados["temperatura_alta"]

    # Dashboard
    dashboard = {
        "cards": {
            "cpu": {
                "percentual": cpu_percentual
            },
            "ram": {
                "percentual": ram_percentual
            },
            "disco": {
                "percentual": disco_percentual
            }
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
            "cpu_porcentagem": linha["cpu_percentual"],
            "ram_porcentagem": linha["ram_percentual"],
            "disco_porcentagem": linha["disco_percentual"],
            "data_hora": linha["data_hora"],
            "cpu_freq": linha["frequencia_atual_ghz"],
            "cpu_freq_max": linha["frequencia_max_ghz"],
            "ram_total": linha["ram_total_gb"],
            "temp_atual": linha["temperatura_atual"],
            "temp_alta": linha["temperatura_alta"]
        })

    # Histórico dos processos
    historico_processos = []

    for _, linha in df_processos_filtrado.iterrows():

        historico_processos.append({
            "pid": linha["pid"],
            "nomeProcesso": linha["nomeProcesso"],
            "consumo_ram": linha["usoMemoriaProcessoMB"],
            "consumo_cpu": linha["usoCpuProcesso"]
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
        Bucket="nome-do-bucket",
        Key=f"displays/{id_display}.json",
        Body=json.dumps(json_final, ensure_ascii=False),
        ContentType="application/json"
    )

    print(f"Display {id_display} enviado com sucesso.")