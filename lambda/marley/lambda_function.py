import os
import json
import boto3
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth

# --- CONFIGURAÇÕES ---
JIRA_EMAIL = os.getenv('JIRA_EMAIL')
JIRA_TOKEN = os.getenv('JIRA_TOKEN')
JIRA_URL = "https://vooh.atlassian.net/rest/api/3/search/jql"
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
MINUTOS_EM_2_SEMANAS = 14 * 24 * 60

s3 = boto3.client('s3')

def obter_dados_jira():
    if not JIRA_EMAIL or not JIRA_TOKEN:
        return {
            "abertos": 0,
            "fechados": 0,
            "mttr": {
                "medio": 0,
                "melhor": 0,
                "pior": 0
            }
        }

    params = {
        "jql": "project = SCRUM",
        "maxResults": 100,
        "fields": "summary,status,created,resolutiondate,priority,description"
    }

    try:
        r = requests.get(
            JIRA_URL,
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN),
            params=params,
            headers={
                "Accept": "application/json"
            }
        )

        if r.status_code != 200:
            return {
                "abertos": 0,
                "fechados": 0,
                "mttr": {"medio": 0, "melhor": 0, "pior": 0}
            }

        dados_jira = r.json()
        issues = dados_jira.get("issues", [])
        dados = []

        for issue in issues:
            fields = issue.get("fields", {})
            
            # Pega a descrição do chamado
            # No Jira v3, a descrição vem em um formato de blocos (ADF), 
            # por isso tentamos pegar o texto de forma simples
            desc_texto = str(fields.get("description", ""))
            
            # Procura por "Zona: " seguido de números na descrição
            import re
            zona_match = re.search(r'Zona:\s*(\d+)', desc_texto, re.IGNORECASE)
            id_zona_jira = int(zona_match.group(1)) if zona_match else None

            dados.append({
                "ticket": issue.get("key"),
                "idZona": id_zona_jira,
                "criado": fields.get("created"),
                "resolvido": fields.get("resolutiondate")
            })

        df_jira = pd.DataFrame(dados)

        abertos = int(df_jira["resolvido"].isna().sum())
        fechados = int(df_jira["resolvido"].notna().sum())

        df_jira["criado"] = pd.to_datetime(df_jira["criado"], errors="coerce")
        df_jira["resolvido"] = pd.to_datetime(df_jira["resolvido"], errors="coerce")

        resolvidos = df_jira[df_jira["resolvido"].notna()].copy()
        mttr_stats = {"medio": 0, "melhor": 0, "pior": 0}
        ranking_mttr = {"melhor_zona": "N/A", "pior_zona": "N/A"}

        if len(resolvidos) > 0:
            # Calcula o tempo de solução em minutos
            resolvidos["mttr_min"] = (resolvidos["resolvido"] - resolvidos["criado"]).dt.total_seconds() / 60
            
            # Estatísticas Gerais
            mttr_stats = {
                "medio": round(float(resolvidos["mttr_min"].mean()), 2),
                "melhor": round(float(resolvidos["mttr_min"].min()), 2),
                "pior": round(float(resolvidos["mttr_min"].max()), 2)
            }

            # --- NOVO: Melhor e Pior Zona por MTTR ---
            # Remove chamados que não conseguimos identificar a Zona
            resolvidos_com_zona = resolvidos.dropna(subset=['idZona'])
            
            if not resolvidos_com_zona.empty:
                # Calcula a média de tempo para cada zona
                medias_por_zona = resolvidos_com_zona.groupby("idZona")["mttr_min"].mean()
                
                id_melhor = medias_por_zona.idxmin()
                id_pior = medias_por_zona.idxmax()
                
                ranking_mttr = {
                    "melhor_zona": f"Zona {int(id_melhor)} (Média: {round(medias_por_zona[id_melhor], 1)} min)",
                    "pior_zona": f"Zona {int(id_pior)} (Média: {round(medias_por_zona[id_pior], 1)} min)"
                }

        return {
            "abertos": abertos,
            "fechados": fechados,
            "mttr_geral": mttr_stats,
            "ranking_zonas": ranking_mttr
        }

    except Exception as e:
        print(f"Erro Jira: {e}")
        return {
            "abertos": 0,
            "fechados": 0,
            "mttr": {
                "medio": 0,
                "melhor": 0,
                "pior": 0
            }
        }

def lambda_handler(event, context):
    try:
        # 1. Jira
        jira_resultado = obter_dados_jira()

        # 2. S3
        prefixo_entrada = "trusted/"
        resposta = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefixo_entrada)
        arquivos = [item['Key'] for item in resposta.get('Contents', []) if item['Key'].endswith('.csv')]

        if not arquivos:
            return {
                "statusCode": 404,
                "body": "Nenhum CSV encontrado."
            }

        lista_dfs = []

        for key in arquivos:
            obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=key)
            df_temp = pd.read_csv(BytesIO(obj['Body'].read()), encoding='utf-8')
            lista_dfs.append(df_temp)

        df = pd.concat(lista_dfs, ignore_index=True)
        df.columns = df.columns.str.strip()
        df['data_hora'] = pd.to_datetime(df['data_hora'], format="%d-%m-%Y %H:%M")
        df = df.sort_values(by=['idZona', 'idDisplay', 'data_hora'])
        df['diferenca'] = df.groupby(['idZona', 'idDisplay'])['data_hora'].diff()
        df['downtime_min'] = 0.0
        mask = df['diferenca'] > pd.Timedelta(minutes=2)
        df.loc[mask, 'downtime_min'] = (df.loc[mask, 'diferenca'].dt.total_seconds() / 60) - 1

        data_fim = df['data_hora'].max()
        data_2w = data_fim - timedelta(days=14)
        data_4w = data_fim - timedelta(days=28)

        metricas_zonas = []

        for zona, dados in df.groupby('idZona'):
            # Quinzena Atual
            recentes = dados[dados['data_hora'] >= data_2w]
            minutos_down_at = int(recentes['downtime_min'].sum())
            n_eventos_at = (recentes['diferenca'] > pd.Timedelta(minutes=2)).sum()
            
            # Quinzena Anterior
            antigos = dados[(dados['data_hora'] >= data_4w) & (dados['data_hora'] < data_2w)]
            minutos_down_ant = int(antigos['downtime_min'].sum())

            # --- CÁLCULOS DE VARIAÇÃO ---
            diff_min = minutos_down_at - minutos_down_ant
            sinal_time = "+" if diff_min >= 0 else "-"
            h_dif, m_dif = divmod(abs(diff_min), 60)
            var_downtime_str = f"{sinal_time}{h_dif}h {m_dif}min"

            # Disponibilidade %
            n_disp = dados['idDisplay'].nunique()
            total_possivel = MINUTOS_EM_2_SEMANAS * n_disp
            
            disp_at = round(max(0, 100 - (minutos_down_at / total_possivel * 100)), 2)
            disp_ant = round(max(0, 100 - (minutos_down_ant / total_possivel * 100)), 2)
            
            diff_pct = round(disp_at - disp_ant, 2)
            var_pct_str = f"{'+' if diff_pct >= 0 else ''}{diff_pct}%"

            # --- SAÚDE OPERACIONAL (Comparação com 14 dias atrás) ---
            saude_pontos = round(diff_pct * 100)

            # --- PROJEÇÃO (PRÓXIMOS 14 DIAS) ---
            proj_disp = round(max(0, min(100, disp_at + diff_pct)), 2)
            minutos_proj = int((1 - (proj_disp / 100)) * total_possivel)
            h_p, m_p = divmod(minutos_proj, 60)

            metricas_zonas.append({
                "idZona": int(zona),
                "total_displays": n_disp,
                "reincidencia": "Sim" if n_eventos_at > 1 else "Não",
                "n_eventos_periodo": int(n_eventos_at),
                "status_atual": {
                    "disponibilidade_pct": disp_at,
                    "downtime_total": f"{minutos_down_at // 60}h {minutos_down_at % 60}min"
                },
                "saude_operacional": {
                    "pontos_vs_14_dias": saude_pontos,
                    "tendencia_14_dias": "melhora" if diff_pct > 0 else "piora" if diff_pct < 0 else "estavel"
                },
                "comparativo_14_dias": {
                    "variacao_downtime_tempo": var_downtime_str,
                    "variacao_disponibilidade_pct": var_pct_str
                },
                "previsao_proximos_14_dias": {
                    "disponibilidade_estimada_pct": proj_disp,
                    "downtime_estimado": f"{h_p}h {m_p}min"
                }
            })

        resultado_final = {
            "dashboard_zonas": metricas_zonas,
            "jira": jira_resultado,
            "ultima_atualizacao": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        s3.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key="client/metricas_dashboard.json",
            Body=json.dumps(resultado_final, indent=4),
            ContentType='application/json'
        )

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(resultado_final, indent=4)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({"erro": str(e)})
        }
