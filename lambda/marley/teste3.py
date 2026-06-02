import os
import json
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. Carregar credenciais do .env
load_dotenv('../.env')

# --- CONFIGURAÇÕES AWS ---
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
MINUTOS_EM_2_SEMANAS = 14 * 24 * 60

s3 = boto3.client('s3')

def processar_dados():
    try:
        # 1. Buscar arquivos na pasta trusted/
        prefixo_entrada = "trusted/"
        print(f"Buscando em: {AWS_BUCKET_NAME}/{prefixo_entrada}")
        
        resposta = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefixo_entrada)
        arquivos = [item['Key'] for item in resposta.get('Contents', []) if item['Key'].endswith('.csv')]
        
        if not arquivos:
            print("Nenhum arquivo encontrado.")
            return

        lista_dfs = []
        for key in arquivos:
            obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=key)
            df_temp = pd.read_csv(BytesIO(obj['Body'].read()), encoding='utf-8')
            lista_dfs.append(df_temp)

        df = pd.concat(lista_dfs, ignore_index=True)
        df.columns = df.columns.str.strip()
        
        # 2. Cálculo de Downtime
        df['data_hora'] = pd.to_datetime(df['data_hora'], format="%d-%m-%Y %H:%M")
        df = df.sort_values(by=['idZona', 'idDisplay', 'data_hora'])

        df['diferenca'] = df.groupby(['idZona', 'idDisplay'])['data_hora'].diff()
        df['downtime_min'] = 0.0
        mask = df['diferenca'] > pd.Timedelta(minutes=2)
        df.loc[mask, 'downtime_min'] = (df.loc[mask, 'diferenca'].dt.total_seconds() / 60) - 1

        # 3. Períodos (Últimos 14 dias e 14 dias anteriores)
        data_fim = df['data_hora'].max()
        data_2w = data_fim - timedelta(days=14)
        data_4w = data_fim - timedelta(days=28)

        resultado_final = []

        # 4. Processar por Zona
        for zona, dados in df.groupby('idZona'):
            # Quinzena Atual
            recentes = dados[dados['data_hora'] >= data_2w]
            minutos_down_at = int(recentes['downtime_min'].sum())
            
            # Quinzena Anterior
            antigos = dados[(dados['data_hora'] >= data_4w) & (dados['data_hora'] < data_2w)]
            minutos_down_ant = int(antigos['downtime_min'].sum())

            # --- CÁLCULOS DE VARIAÇÃO ---
            # Variação Downtime (Tempo)
            diff_min = minutos_down_at - minutos_down_ant
            sinal_time = "+" if diff_min >= 0 else "-"
            h_dif, m_dif = divmod(abs(diff_min), 60)
            var_downtime_str = f"{sinal_time}{h_dif}h {m_dif}min"

            # Disponibilidade %
            n_disp = dados['idDisplay'].nunique()
            total_possivel = MINUTOS_EM_2_SEMANAS * n_disp
            
            disp_at = round(max(0, 100 - (minutos_down_at / total_possivel * 100)), 2)
            disp_ant = round(max(0, 100 - (minutos_down_ant / total_possivel * 100)), 2)
            
            # Variação Disponibilidade (%)
            diff_pct = round(disp_at - disp_ant, 2)
            var_pct_str = f"{'+' if diff_pct >= 0 else ''}{diff_pct}%"

            # --- PROJEÇÃO (PRÓXIMOS 14 DIAS) ---
            proj_disp = round(max(0, min(100, disp_at + diff_pct)), 2)
            minutos_proj = int((1 - (proj_disp / 100)) * total_possivel)
            h_p, m_p = divmod(minutos_proj, 60)

            resultado_final.append({
                "idZona": int(zona),
                "total_displays": n_disp,
                "status_atual": {
                    "disponibilidade_pct": disp_at,
                    "downtime_total": f"{minutos_down_at // 60}h {minutos_down_at % 60}min"
                },
                "comparativo_14_dias": {
                    "variacao_downtime_tempo": var_downtime_str,
                    "variacao_disponibilidade_pct": var_pct_str,
                    "tendencia": "melhora" if diff_pct > 0 else "piora" if diff_pct < 0 else "estavel"
                },
                "previsao_proximos_14_dias": {
                    "disponibilidade_estimada_pct": proj_disp,
                    "downtime_estimado": f"{h_p}h {m_p}min"
                }
            })

        # 5. Salvar e Enviar
        json_output = json.dumps(resultado_final, indent=4)
        s3.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key="client/metricas_dashboard.json",
            Body=json_output,
            ContentType='application/json'
        )
        print("Sucesso! JSON atualizado com variação de tempo e porcentagem.")

    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    processar_dados()
