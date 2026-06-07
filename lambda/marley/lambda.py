import datetime
import json
import os
import re
from datetime import timedelta, datetime
from typing import Any, Dict

import boto3
import pandas as pd
import requests
import pymysql

from dotenv import load_dotenv  # Remover
from pandas import DataFrame
from requests.auth import HTTPBasicAuth
from io import BytesIO

load_dotenv("../../.env")  # Remover

# E-mail da conta Jira
JIRA_EMAIL = os.getenv('JIRA_EMAIL')

# Token de acesos ao Jira
JIRA_TOKEN = os.getenv('JIRA_TOKEN')

# URL da API do Jira
JIRA_API_URL = "https://vooh.atlassian.net/rest/api/3/search/jql"

# Nome do Bucket da AWS
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')

# Diretório de input dos dados
AWS_BUCKET_INPUT_DIR = "trusted/"

# Diretório de output dos dados
AWS_BUCKET_OUTPUT_DIR = "client/"

# Hostname do banco de dados
DB_HOSTNAME = os.getenv("DB_HOSTNAME")

# Porta de conexão com o banco de dados
DB_PORT = os.getenv("DB_PORT")

# Usuário do banco de dados
DB_USER = os.getenv("DB_USER")

# Senha do banco de dados
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Banco de dados
DB_DATABASE = os.getenv("DB_DATABASE")

# SLA de tempo de resolução
TEMPO_SLA = 45

# Períodos
PERIODOS: Dict[str, int] = {
    "semanal": 7,
    "duas_semanas": 14,
    "mensal": 30
}


# Acesso ao banco de dados
def _dados_banco() -> dict:
    """
    Função para buscar dados no banco de dados referente aos parâmetros de contrato
    """
    try:
        conexao = pymysql.connect(
            host=DB_HOSTNAME,
            port=int(DB_PORT),
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            cursorclass=pymysql.cursors.DictCursor
        )

        dados = {"disponibilidade": 95.0, "sla": 45} # Valores padrão caso falhe

        with conexao.cursor() as cursor:
            SQL = "SELECT metaDisponibilidade as disponibilidade, sla FROM contrato WHERE fkEmpresa = 1 LIMIT 1;"
            cursor.execute(SQL)
            resultado = cursor.fetchone()
            if resultado:
                dados = resultado

        return dados

    except Exception as erro:
        print(f"Erro ao conectar com banco de dados: {erro}")
        return {"disponibilidade": 95.0, "sla": 45}

    finally:
        if 'conexao' in locals() and conexao.open:
            conexao.close()


def _buscar_chamados_jira() -> pd.DataFrame:
    """
    Busca todos os chamados do Jira e retorna um DataFrame bruto.
    """
    if not JIRA_EMAIL or not JIRA_TOKEN:
        return pd.DataFrame()

    params = {
        "jql": "project = SCRUM",
        "maxResults": 500,
        "fields": "summary,status,created,resolutiondate,priority,description"
    }

    try:
        req = requests.get(JIRA_API_URL, auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN), params=params, headers={"Accept": "application/json"})
        if req.status_code != 200:
            return pd.DataFrame()

        chamados = req.json().get("issues", [])
        lista_dados = []

        for chamado in chamados:
            campos = chamado.get("fields", {})
            descricao = str(campos.get("description", ""))
            zona_match = re.search(r'Zona:\s*(\d+)', descricao, re.IGNORECASE)
            id_zona = int(zona_match.group(1)) if zona_match else None

            lista_dados.append({
                "ticket": chamado.get("key"),
                "id_zona": id_zona,
                "criado": campos.get("created"),
                "resolvido": campos.get("resolutiondate")
            })

        df = pd.DataFrame(lista_dados)
        df["criado"] = pd.to_datetime(df["criado"], errors="coerce", utc=True)
        df["resolvido"] = pd.to_datetime(df["resolvido"], errors="coerce", utc=True)
        return df
    except Exception:
        return pd.DataFrame()


def _calcular_metricas_jira(df_jira: pd.DataFrame, data_inicio: datetime, data_fim: datetime, tempo_sla: int) -> dict:
    """
    Calcula o resumo do Jira filtrado por um período específico.
    """
    resumo = {
        "incidentes": {"abertos": 0, "fechados": 0, "percentual_dentro_sla": 0},
        "mttr": {
            "medio_min": 0, "melhor_min": 0, "pior_min": 0, "meta_min": tempo_sla,
            "melhor_zona": {"zona": "N/A", "mttr_min": 0},
            "pior_zona": {"zona": "N/A", "mttr_min": 0}
        }
    }

    if df_jira is None or df_jira.empty:
        return resumo

    # Criamos uma cópia e removemos o fuso horário de todas as colunas de data
    df_p = df_jira.copy()
    df_p["criado"] = df_p["criado"].dt.tz_localize(None)
    df_p["resolvido"] = df_p["resolvido"].dt.tz_localize(None)
    
    # 1. Incidentes Abertos: Criados dentro do período
    mask_abertos = (df_p["criado"] >= data_inicio) & (df_p["criado"] <= data_fim)
    resumo["incidentes"]["abertos"] = int(mask_abertos.sum())

    # 2. Incidentes Fechados e MTTR: Resolvidos dentro do período
    mask_resolvidos = (df_p["resolvido"] >= data_inicio) & (df_p["resolvido"] <= data_fim)
    resolvidos = df_p[mask_resolvidos].copy()
    
    resumo["incidentes"]["fechados"] = len(resolvidos)

    if not resolvidos.empty:
        # Cálculo do MTTR em minutos (usando 1 casa decimal para evitar o "0" absoluto)
        resolvidos["mttr_min"] = (resolvidos["resolvido"] - resolvidos["criado"]).dt.total_seconds() / 60
        
        resumo["mttr"]["medio_min"] = round(resolvidos["mttr_min"].mean(), 1)
        resumo["mttr"]["pior_min"] = round(resolvidos["mttr_min"].max(), 1)
        resumo["mttr"]["melhor_min"] = round(resolvidos["mttr_min"].min(), 1)

        # % dentro da meta de SLA do banco
        dentro = int((resolvidos["mttr_min"] <= tempo_sla).sum())
        resumo["incidentes"]["percentual_dentro_sla"] = round((dentro / len(resolvidos)) * 100, 1)

        # Melhor e Pior Zona baseado no MTTR médio do período
        df_z = resolvidos.groupby("id_zona")["mttr_min"].mean().reset_index()
        if not df_z.empty:
            linha_melhor = df_z["mttr_min"].idxmin()
            linha_pior = df_z["mttr_min"].idxmax()
            resumo["mttr"]["melhor_zona"] = {
                "zona": int(df_z.loc[linha_melhor, "id_zona"]), 
                "mttr_min": round(df_z.loc[linha_melhor, "mttr_min"], 1)
            }
            resumo["mttr"]["pior_zona"] = {
                "zona": int(df_z.loc[linha_pior, "id_zona"]), 
                "mttr_min": round(df_z.loc[linha_pior, "mttr_min"], 1)
            }

    return resumo


def _dados_jira() -> str:
    """
    Função legada para manter compatibilidade no main
    """
    df = _buscar_chamados_jira()
    metricas = _calcular_metricas_jira(df, datetime.now() - timedelta(days=30), datetime.now(), 45)
    return json.dumps(metricas)


# Calcular o Downtime
def _calcular_downtime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula os gaps entre capturas e calcula o downtime em minutos

    A captura normal deve ocorrer a cada 1 minuto
    Se a diferença entre dois registros for maior que 2 minutos,
    o display ficou offline. O downtime real é: gap - 1 min.

    :param df:
    :return: Data Frame
    """
    agrupamentos = ["idEmpresa", "idZona", "idDisplay"]

    df["diferenca"] = df.groupby(agrupamentos)["data_hora"].diff()

    df["minutos_downtime"] = 0.0

    # Linhas onde o gap foi maior que 2 minutos = downtime
    gaps = df["diferenca"] > pd.Timedelta(minutes=2)

    # Filtra os gaps
    df.loc[gaps, "minutos_downtime"] = (df.loc[gaps, "diferenca"].dt.total_seconds() / 60) - 1

    return df


# Calcular disponibilidade agredada
def _calcular_disponibilidade_agregada(df_periodo: pd.DataFrame, dias: int) -> float:
    """
    Calcula a disponibilidade real no período

    Fórmula: disponibilidade = (1 - downtime_total / temmpo_total_possivel) * 100

    Com tempo_total_disponivel = minutos_no_periodo * numero_de_displays

    :param df_periodo:
    :param dias:
    :return:
    """

    # Retorna 100% caso o período seja vazio
    if df_periodo.empty:
        return 100.0

    # Conta o total de displays
    total_displays = df_periodo["idDisplay"].nunique()

    # Minutos totais no período
    minutos_no_periodo = dias * 24 * 60

    # Tempo total calculado
    tempo_total = total_displays * minutos_no_periodo

    # Downtime total
    downtime_total = df_periodo["minutos_downtime"].sum()

    # Percentual de disponibilidade
    disponibilidade = max(0.0, (1 - downtime_total / tempo_total) * 100)

    # Retorna a disponibilidade em percentual com duas casas arredondadas
    return round(float(disponibilidade), 2)


# Cálculo da saúde operacional
def _calcular_saude_operacional(
    disponibilidade: float,
    percentual_downtime: float,
    mttr_min: float,
    total_reincidencias: int,
) -> tuple:
    """
    Calcula a nota de saúde operacional (0 a 100) e o status textual.

    Cada componente tem uma faixa de pontos:
      - Disponibilidade: até 45 pts
      - Downtime %: até 25 pts
      - MTTR: até 20 pts
      - Reincidências: até 10 pts
    """

    # Disponibilidade
    if disponibilidade >= 99:
        pts_disp = 45
    elif disponibilidade >= 97:
        pts_disp = 38
    elif disponibilidade >= 95:
        pts_disp = 30
    elif disponibilidade >= 90:
        pts_disp = 20
    else:
        pts_disp = 10

    # % de downtime sobre o total de registros
    if percentual_downtime <= 0.01:
        pts_down = 25
    elif percentual_downtime <= 0.03:
        pts_down = 18
    elif percentual_downtime <= 0.05:
        pts_down = 10
    else:
        pts_down = 5

    # MTTR
    if mttr_min <= 35:
        pts_mttr = 20
    elif mttr_min <= 60:
        pts_mttr = 15
    elif mttr_min <= 120:
        pts_mttr = 8
    else:
        pts_mttr = 3

    # Reincidências
    if total_reincidencias == 0:
        pts_reinc = 10
    elif total_reincidencias <= 2:
        pts_reinc = 7
    elif total_reincidencias <= 5:
        pts_reinc = 4
    else:
        pts_reinc = 1

    # Nota final
    nota = pts_disp + pts_down + pts_mttr + pts_reinc

    # Status textual da nota
    if nota >= 85:
        status = "saudável"
    elif nota >= 70:
        status = "degradada"
    elif nota >= 50:
        status = "crítica"
    else:
        status = "colapso"

    # Retorna a nota e o status textual
    return nota, status


def _calcular_risco_operacional(
    disp_atual: float,
    disp_anterior: float,
    down_atual: float,
    down_anterior: float,
    mttr_atual: float,
    mttr_anterior: float,
    reincidencias: int,
    dias_para_violacao: Any = None
) -> tuple:
    """
    Calcula o nível de risco comparando o período atual com o anterior
    e também a proximidade da violação da meta (SLA).
    """

    # Score baseado em tendência (Comparação com o passado)
    score = 0

    if disp_atual < disp_anterior:
        score += 35  # disponibilidade piorou

    if down_atual > down_anterior:
        score += 25  # mais downtime que antes

    if mttr_atual > mttr_anterior:
        score += 20  # resolução mais lenta

    if reincidencias >= 5:
        score += 20  # muitas quedas no período

    # Nível de risco inicial baseado no score de tendência
    if score >= 70:
        nivel = "alto"
    elif score >= 40:
        nivel = "médio"
    else:
        nivel = "baixo"

    # Ajuste de urgência baseado no tempo restante para violação (SLA)
    # A urgência de contrato SEMPRE tem prioridade sobre a tendência
    if dias_para_violacao is not None:
        if dias_para_violacao == 0:
            nivel = "alto"  # Já violou ou vai violar hoje
        elif dias_para_violacao <= 3:
            nivel = "alto"  # Risco iminente (menos de 3 dias de 'saldo')
        elif dias_para_violacao <= 7:
            # Se o risco era baixo pela tendência, sobe para médio pelo prazo curto
            if nivel == "baixo":
                nivel = "médio"

    # Retorna o score e o nível de risco final
    return score, nivel

def _calcular_metricas_zonas(
        df_periodo: pd.DataFrame,
        df_periodo_anterior: pd.DataFrame,
        total_displays_empresa: int,
        dias: int
) -> list:
    """
    Para cada zona calcula:
    - Disponibilidade agregada (atual e anterior)
    - Downtime total em horas/minutos
    - Reincidências (número de eventos de queda)
    - Índice de risco (alto / médio / baixo)
    - Tendência (série de eventos por dia + projeção)
    - Displays afetados

    :param df_periodo:
    :param df_periodo_anterior:
    :param total_displays_empresa:
    :param dias:
    :return:
    """

    # Lista de zonas
    zonas = []

    # Percorre cada item dos dados de zona agrupado por ID da empresa e ID da Zona
    for (id_empresa, id_zona), dados_zona in df_periodo.groupby(["idEmpresa", "idZona"]):
        dados_zona_anterior = df_periodo_anterior[(df_periodo_anterior["idEmpresa"] == id_empresa) & (df_periodo_anterior["idZona"] == id_zona)]

        # Downtime
        downtime_atual = int(dados_zona["minutos_downtime"].sum())
        downtime_anterior = int(dados_zona_anterior["minutos_downtime"].sum())

        # Conversão de horas e minutos
        horas_down = downtime_atual // 60
        min_down = downtime_atual % 60

        # Disponibilidade agregada
        numero_displays = dados_zona["idDisplay"].nunique()
        tempo_total = numero_displays * dias * 24 * 60

        # Disponibilidade atual
        disponibilidade_atual = round(max(0.0, (1 - downtime_atual / tempo_total) * 100), 2)

        # Disponibilidade anterior
        disponibilidade_anterior = round(max(0.0, (1 - downtime_anterior / tempo_total) * 100), 2)

        # Variação de disponibilidade
        variacao_disponibilidade = round(disponibilidade_atual - disponibilidade_anterior, 2)

        # Reincidências (cada registro com downtime > 0)
        eventos = dados_zona[dados_zona["minutos_downtime"] > 0]

        # Total de reicidências
        reincidencias = len(eventos)

        # Peso de riscos das zonas
        if reincidencias >= 5:
            risco_zona = "alto"
        elif reincidencias >= 3:
            risco_zona = "médio"
        else:
            risco_zona = "baixo"

        # Tendências de eventos
        tendencia_real = (eventos.groupby(eventos["data_hora"].dt.date).size().tolist())

        # Projeção de tendência
        projecao_tendencia = []

        if len(tendencia_real) >= 2:
            delta = tendencia_real[-1] - tendencia_real[-2]
            ultimo = tendencia_real[-1]

            for _ in range(4):
                ultimo = max(0, ultimo + delta)
                projecao_tendencia.append(ultimo)

        # Tendência textual
        if variacao_disponibilidade > 0:
            tendencia_texto = "melhora"
        elif variacao_disponibilidade < 0:
            tendencia_texto = "piora"
        else:
            tendencia_texto = "estável"

        # Displays afetados
        displays_com_downtime = int(
            dados_zona[dados_zona["minutos_downtime"] > 0]["idDisplay"].nunique()
        )

        # Adiciona os dados na lista
        zonas.append({
            "zona": f"Zona {id_zona}",
            "indice_risco": risco_zona,
            "reincidencias": reincidencias,
            "downtime": {
                "minutos": downtime_atual,
                "formatado": f"{horas_down}h {min_down:02d}min",
            },
            "disponibilidade": {
                "atual_percentual": disponibilidade_atual,
                "anterior_percentual": disponibilidade_anterior,
                "variacao_percentual": variacao_disponibilidade,
                "tendencia": tendencia_texto
            },
            "displays": {
                "total": numero_displays,
                "afetados": displays_com_downtime,
            },
            "tendencia_eventos": {
                "real": tendencia_real,
                "projecao": projecao_tendencia
            }
        })

    # Retorna a lista dos dados de zonas
    return zonas

# Calcular série de dados diários para gráfico de disponibiliade
def _calcular_serie_diaria(
    df_periodo: pd.DataFrame,
    dias: int
) -> dict:
    """
    Calcula a disponibilidade agregada para cada dia do período.
    Retorna labels (datas) e valores separados para o gráfico.
    """

    labels = []
    valores_reais = []

    # Agrupa os dados dia a dia
    agrupado = df_periodo.groupby(df_periodo["data_hora"].dt.date)

    for data, dados_dia in agrupado:
        disponibilidade_dia = _calcular_disponibilidade_agregada(dados_dia, 1)
        labels.append(str(data))
        valores_reais.append(disponibilidade_dia)

    # Projeção simples: repete a tendência dos últimos 4 dias
    projecao = []
    if len(valores_reais) >= 2:
        tendencia = valores_reais[-1] - valores_reais[-2]
        ultimo    = valores_reais[-1]
        for _ in range(4):
            ultimo = round(min(100.0, max(0.0, ultimo + tendencia)), 2)
            projecao.append(ultimo)

    return {
        "labels":    labels,
        "real":      valores_reais,
        "projecao":  projecao,
    }


# Gera um JSON de todos os dados
def _gerar_json_dashboard(df: pd.DataFrame, meta: float, tempo_sla: int) -> dict:
    """
    Monta um JSON completo para o dashboard com três períodos:
    Semanal, Quinzenal (14 dias) e Mensal
    """
    data_fim = df["data_hora"].max()
    # Usamos a data atual para o Jira para capturar chamados recentes, independente da data do CSV
    data_fim_jira = datetime.now()
    
    # Se os dados_jira vierem como string (JSON), mantemos por compatibilidade,
    # mas o ideal é que o motor use o DataFrame bruto de chamados.
    df_chamados = _buscar_chamados_jira()

    resultado = {
        "periodos": {},
        "ultima_atualizacao": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "jira": _calcular_metricas_jira(df_chamados, data_fim_jira - timedelta(days=30), data_fim_jira, tempo_sla)
    }

    total_displays = df["idDisplay"].nunique()

    for nome_periodo, dias in PERIODOS.items():
        data_inicio = data_fim - timedelta(days=dias)
        data_inicio_anterior = data_inicio - timedelta(days=dias)
        
        # Datas específicas para o filtro do Jira
        data_inicio_jira = data_fim_jira - timedelta(days=dias)
        data_inicio_anterior_jira = data_inicio_jira - timedelta(days=dias)

        df_atual = df[df["data_hora"] >= data_inicio].copy()
        df_anterior = df[(df["data_hora"] >= data_inicio_anterior) & (df["data_hora"] < data_inicio)].copy()

        if df_atual.empty:
            continue

        # Cálculos de disponibilidade
        disp_atual = _calcular_disponibilidade_agregada(df_periodo=df_atual, dias=dias)
        disp_anterior = _calcular_disponibilidade_agregada(df_periodo=df_anterior, dias=dias)
        delta_meta = round(disp_atual - meta, 2)

        # Downtime
        downtime_atual_min = int(df_atual["minutos_downtime"].sum())
        downtime_anterior_min = int(df_anterior["minutos_downtime"].sum())
        diferenca_downtime_min = downtime_atual_min - downtime_anterior_min

        # Métricas do Jira para este período específico (Usando as datas do Jira)
        jira_atual = _calcular_metricas_jira(df_chamados, data_inicio_jira, data_fim_jira, TEMPO_SLA)
        jira_anterior = _calcular_metricas_jira(df_chamados, data_inicio_anterior_jira, data_inicio_jira, TEMPO_SLA)

        mttr_atual = jira_atual["mttr"]["medio_min"]
        mttr_anterior = jira_anterior["mttr"]["medio_min"]

        # Percentual de Downtime sobre o TEMPO TOTAL POSSÍVEL (não sobre o número de linhas)
        minutos_possiveis = total_displays * dias * 24 * 60
        percentual_downtime_frac = downtime_atual_min / max(minutos_possiveis, 1)

        total_reincidencias = int((df_atual["minutos_downtime"] > 0).sum())

        # Saúde operacional
        nota_saude, status_saude = _calcular_saude_operacional(
            disponibilidade=disp_atual,
            percentual_downtime=percentual_downtime_frac,
            mttr_min=mttr_atual,
            total_reincidencias=total_reincidencias
        )

        nota_anterior, _ = _calcular_saude_operacional(
            disponibilidade=disp_anterior,
            percentual_downtime=(downtime_anterior_min / max(minutos_possiveis, 1)),
            mttr_min=mttr_anterior,
            total_reincidencias=int((df_anterior["minutos_downtime"] > 0).sum())
        )

        variacao_saude = round(nota_saude - nota_anterior, 1)

        # Dias estimados até violação de disponibilidade (SLA)
        media_down_dia = downtime_atual_min / max(dias, 1)
        
        # Limite permitido de downtime para o período proporcional (se for 7 dias, calcula 7 dias de crédito)
        sla_limite_min_periodo = (1 - (meta / 100)) * minutos_possiveis
        restante_min = max(0, sla_limite_min_periodo - downtime_atual_min)

        if media_down_dia > 0:
            if downtime_atual_min >= sla_limite_min_periodo:
                dias_violacao = 0
            else:
                dias_violacao = int(restante_min / media_down_dia)
        else:
            dias_violacao = None

        # Nível de risco operacional (Agora considerando o tempo para violação)
        _, nivel_risco = _calcular_risco_operacional(
            disp_atual=disp_atual,
            disp_anterior=disp_anterior,
            down_atual=downtime_atual_min,
            down_anterior=downtime_anterior_min,
            mttr_atual=mttr_atual,
            mttr_anterior=mttr_anterior,
            reincidencias=total_reincidencias,
            dias_para_violacao=dias_violacao
        )

        serie = _calcular_serie_diaria(df_periodo=df_atual, dias=dias)
        zonas = _calcular_metricas_zonas(df_atual, df_anterior, total_displays, dias)

        resultado["periodos"][nome_periodo] = {
            "saude_operacional": {
                "nota": nota_saude,
                "status": status_saude,
                "variacao_pts": variacao_saude,
                "label_variacao": f"{'+' if variacao_saude >= 0 else ''}{variacao_saude} pts vs {dias} dias atrás",
            },
            "uptime": {
                "atual_pct": disp_atual,
                "meta_pct": meta,
                "delta_meta": delta_meta,
                "label_delta": f"{'▼' if delta_meta < 0 else '▲'} {abs(delta_meta)}% {'abaixo' if delta_meta < 0 else 'acima'} da meta",
            },
            "downtime": {
                "atual_min": downtime_atual_min,
                "formatado": f"{downtime_atual_min // 60}h {downtime_atual_min % 60:02d}min",
                "variacao_min": diferenca_downtime_min,
                "label_variacao": f"{'▼' if diferenca_downtime_min <= 0 else '▲'} {abs(diferenca_downtime_min // 60)}h que no período anterior",
                "projecao_min": max(0, downtime_atual_min + diferenca_downtime_min),
                "projecao_fmt": f"{(max(0, downtime_atual_min + diferenca_downtime_min)) // 60}h {(max(0, downtime_atual_min + diferenca_downtime_min)) % 60:02d}min",
            },
            "risco_operacional": {
                "nivel": nivel_risco,
                "dias_para_violacao": dias_violacao,
                "label": f"Em {dias_violacao} dias no ritmo atual" if dias_violacao is not None else "Sem risco no período",
            },
            "mttr": jira_atual["mttr"],
            "incidentes": jira_atual["incidentes"],
            "grafico_disponibilidade": serie,
            "zonas": zonas
        }

    return resultado



# Leitura do CSV
def _carregar_csv_local(caminho: str) -> pd.DataFrame:
    """
    Lê um CSV local e prepara o DataFrame.

    Returns: Data Frame
    """

    df = pd.read_csv(caminho, encoding="utf-8")
    df.columns = df.columns.str.strip()
    df["data_hora"] = pd.to_datetime(df["data_hora"], format="%d-%m-%Y %H:%M")
    df = df.sort_values(by=["idEmpresa", "idZona", "idDisplay", "data_hora"])
    df = df.reset_index(drop=True)

    return df



# Execução local
if __name__ == "__main__":

    print("[1/4] Lendo CSV Local...")
    df = _carregar_csv_local("dados_calibracao.csv")

    print("[2/4] Calculando matriz de downtime...")
    df = _calcular_downtime(df=df)

    print("[3/4] Consultando parâmetros contratuais no Banco de Dados...")
    dados_banco = _dados_banco()
    meta_disponibilidade = dados_banco["disponibilidade"]
    tempo_sla_banco = dados_banco["sla"]

    print("[4/4] Compilando motor de métricas e gerando estrutura do Dashboard...")
    dashboard = _gerar_json_dashboard(
        df=df,
        meta=meta_disponibilidade,
        tempo_sla=tempo_sla_banco
    )

    print("\n[Escrita] Gravando arquivo de saída local 'dashboard.json'...")
    with open("operacao.json", "w", encoding="utf-8") as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False)

    print("\n Sucesso! O arquivo 'dashboard.json' foi gerado localmente e está pronto.")

    
# Função para AWS
def lambda_handler(event, context):
    """
    Função Lambda AWS para processar os dados e gerar um JSON

    Author: Marley de S. Santos | GitHub: https://github.com/MarleyS439

    :param event:
    :param context:
    :return:
    """
    try:
        # Busca os parâmetros de contrato no banco de dados (Query única e segura)
        dados_banco = _dados_banco()
        meta_disponibilidade = dados_banco["disponibilidade"]
        tempo_sla_banco = dados_banco["sla"]

        # Faz uma requisição no Jira passando o SLA obtido do banco e salva os dados numa variável
        resultado_jira = _dados_jira()

        # Inicializa o cliente do S3
        s3_client = boto3.client("s3")

        # Faz uma requisição no S3 em um bucket para listar os arquivos de um diretório
        req = s3_client.list_objects_v2(
            Bucket=AWS_BUCKET_NAME,
            Prefix=AWS_BUCKET_INPUT_DIR,
        )

        # Lista de arquivos do diretório no bucket S3
        arquivos = []

        # Percorre cada arquivo no resultado da requisição no S3 ou lista vazia caso não tenha nada
        for item in req.get("Contents", []):

            # Chave do arquivo
            arquivo = item["Key"]

            # Caso tenha um arquivo .CSV, adiciona o arquivo na lista de arquivos
            if arquivo.endswith(".csv"):
                arquivos.append(arquivo)

        # Caso não tenha arquivos, retorna um Status Code 404
        if not arquivos:
            return {
                "statusCode": 404,
                "body": json.dumps("Nenhum CSV encontrado")
            }

        # Lista de Data Frames
        data_frames = []

        # Percorre cada arquivo no bucket S3 e faz leitura na memória RAM para velocidade, sem salvar em disco
        for key in arquivos:
            # Obtém o objeto que possui a chave passada
            objeto = s3_client.get_object(
                Bucket=AWS_BUCKET_NAME,
                Key=key
            )

            # Cria um Data Frame temporário na memória RAM com o conteúdo do objeto (.CSV)
            df_temporario = pd.read_csv(
                BytesIO(objeto["Body"].read()),
                encoding="utf-8"
            )

            # Adiciona na lista de Data Frames o Data Frame temporário
            data_frames.append(df_temporario)

        # Junta cada Data Frame em um só (Executado apenas uma vez fora do loop dos arquivos)
        df = pd.concat(data_frames, ignore_index=True)

        # Remove os espaços nas colunas dos Data Frames
        df.columns = df.columns.str.strip()

        # Converte a coluna de data e hora para tipo datetime
        df["data_hora"] = pd.to_datetime(df["data_hora"], format="%d-%m-%Y %H:%M")

        # Ordena o Data Frame pelo ID da Empresa, ID da Zona, ID do Display e a Data e Hora
        df = df.sort_values(by=["idEmpresa", "idZona", "idDisplay", "data_hora"]).reset_index(drop=True)

        # Calcula a diferença entre cada registro de forma agrupada por ID da Empresa, ID da Zona e ID do Display
        df["diferenca"] = df.groupby(by=["idEmpresa", "idZona", "idDisplay"])["data_hora"].diff()

        # Remove as colunas desnecessárias salvando a alteração no DataFrame
        df = df.drop([
            "total_disco_gb", "disco_usado_gb",
            "disco_livre_gb", "disco_percentual",
            "processador_nome", "nucleos_fiscos",
            "nucleos_totais", "frequencia_max_ghz",
            "frequencia_atual_ghz", "cpu_percentual",
            "ram_total_gb", "ram_disponivel_gb",
            "ram_percentual", "upload_mb",
            "download_mb", "errin",
            "dropin", "mtu", "latencia", "conn_established",
            "conn_listen", "conn_time_wait",
            "conn_close_wait", "conn_syn_sent",
            "mac", "ip", "boot_time"
        ], axis=1)

        # Cria a coluna de Downtime
        df["minutos_downtime"] = 0.0

        # Cria uma validação para caso a coluna 'diferenca' tenha tempo maior que 2 minutos (> 1 min entre leituras)
        validacao = df["diferenca"] > pd.Timedelta(minutes=2)

        # Filtra apenas os registros que a diferença entre capturas seja superior a 2 minutos com a correção do tempo
        df.loc[validacao, "minutos_downtime"] = (df.loc[validacao, "diferenca"].dt.total_seconds() / 60) - 1

        # Aciona o motor compilador para calcular os períodos e estruturar o dicionário final do dashboard
        dashboard_final = _gerar_json_dashboard(
            df=df,
            meta=meta_disponibilidade,
            tempo_sla=tempo_sla_banco
        )

        # Grava o JSON gerado de volta no S3 na pasta de saída (/client) para o consumo do front-end
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=f"{AWS_BUCKET_OUTPUT_DIR}operacao.json",
            Body=json.dumps(dashboard_final, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )

        # Retorna o Status 200 de sucesso para a AWS
        return {
            "statusCode": 200,
            "body": json.dumps("Dashboard processado e atualizado no S3 com sucesso!")
        }

    except Exception as erro:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(
                {
                    "erro": str(erro)
                }
            )
        }