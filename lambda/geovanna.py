import pandas as pd

# Carregar o arquivo
df_dados = pd.read_csv('src/transform/dados_empresa2_02_06_2026.csv')
df_processos = pd.read_csv('src/transform/processos_empresa2_02_06_2026.csv')

colunas = ['data_hora', 'processador_nome', 'frequencia_max_ghz', 
               'frequencia_atual_ghz', 'cpu_percentual', 'ram_total_gb',
               'ram_disponivel_gb', 'ram_percentual']

print(df_dados[colunas])

#eu devo utilizar o idDisplay como validação para a exibição?