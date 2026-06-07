
import json
from datetime import timedelta
from io import BytesIO

import pandas as pd


# =========================================
# CONFIGURAÇÕES
# =========================================

META_UPTIME = 99
META_MTTR = 35

PERIODOS = {
    "semanal": 7,
    "quinzenal": 14,
    "mensal": 30
}


# =========================================
# LEITURA CSV
# =========================================

def carregar_csv(path_csv: str) -> pd.DataFrame:

    df = pd.read_csv(path_csv, encoding="utf-8")

    df.columns = df.columns.str.strip()

    df["data_hora"] = pd.to_datetime(
        df["data_hora"],
        format="%d-%m-%Y %H:%M"
    )

    df.sort_values(
        by=["idZona", "idDisplay", "data_hora"],
        inplace=True
    )

    return df


# =========================================
# DOWNTIME
# =========================================

def calcular_downtime(df: pd.DataFrame) -> pd.DataFrame:

    df["diferenca"] = (
        df.groupby(
            ["idZona", "idDisplay"]
        )["data_hora"].diff()
    )

    df["minutos_downtime"] = 0

    validacao = (
        df["diferenca"] >
        pd.Timedelta(minutes=2)
    )

    df.loc[
        validacao,
        "minutos_downtime"
    ] = (
        (
            df.loc[
                validacao,
                "diferenca"
            ].dt.total_seconds() / 60
        ) - 1
    )

    return df


# =========================================
# UPTIME
# =========================================

def calcular_uptime(df_periodo: pd.DataFrame) -> float:

    if df_periodo.empty:
        return 0

    total_registros = len(df_periodo)

    downtime = df_periodo[
        "minutos_downtime"
    ].sum()

    uptime = (
        (
            total_registros - downtime
        ) / total_registros
    ) * 100

    return round(uptime, 2)


# =========================================
# DISPONIBILIDADE
# =========================================

def calcular_disponibilidade(
        df_periodo: pd.DataFrame
) -> dict:

    disponibilidade = {}

    agrupado = df_periodo.groupby(
        df_periodo["data_hora"].dt.date
    )

    for data, dados in agrupado:

        total = len(dados)

        downtime = dados[
            "minutos_downtime"
        ].sum()

        uptime = (
            (
                total - downtime
            ) / total
        ) * 100

        disponibilidade[str(data)] = round(
            uptime,
            2
        )

    return disponibilidade


# =========================================
# REINCIDÊNCIA
# =========================================

def calcular_reincidencias(
        df_periodo: pd.DataFrame
) -> dict:

    eventos = df_periodo[
        df_periodo["minutos_downtime"] > 0
    ]

    agrupado = eventos.groupby(
        "idZona"
    ).size()

    reincidencias = {}

    for zona, total in agrupado.items():

        reincidencias[
            f"Zona {zona}"
        ] = int(total)

    return reincidencias


# =========================================
# MTTR
# =========================================

def calcular_mttr(
        df_jira: pd.DataFrame
) -> float:

    if df_jira.empty:
        return 0

    df_jira["criado"] = pd.to_datetime(
        df_jira["criado"]
    )

    df_jira["resolvido"] = pd.to_datetime(
        df_jira["resolvido"]
    )

    df_jira["tempo_resolucao"] = (
        df_jira["resolvido"] -
        df_jira["criado"]
    ).dt.total_seconds() / 60

    return round(
        df_jira["tempo_resolucao"].mean(),
        2
    )


# =========================================
# NOTA SAÚDE OPERACIONAL
# =========================================

def calcular_nota_saude(
        uptime: float,
        percentual_downtime: float,
        mttr: float,
        reincidencias: int
):

    # UPTIME
    if uptime >= 99:
        pontos_uptime = 45

    elif uptime >= 97:
        pontos_uptime = 38

    elif uptime >= 95:
        pontos_uptime = 30

    elif uptime >= 90:
        pontos_uptime = 20

    else:
        pontos_uptime = 10

    # DOWNTIME
    if percentual_downtime <= 0.01:
        pontos_downtime = 25

    elif percentual_downtime <= 0.03:
        pontos_downtime = 18

    elif percentual_downtime <= 0.05:
        pontos_downtime = 10

    else:
        pontos_downtime = 5

    # MTTR
    if mttr <= 35:
        pontos_mttr = 20

    elif mttr <= 60:
        pontos_mttr = 15

    elif mttr <= 120:
        pontos_mttr = 8

    else:
        pontos_mttr = 3

    # REINCIDÊNCIA
    if reincidencias == 0:
        pontos_reincidencia = 10

    elif reincidencias <= 2:
        pontos_reincidencia = 7

    elif reincidencias <= 5:
        pontos_reincidencia = 4

    else:
        pontos_reincidencia = 1

    nota = (
        pontos_uptime +
        pontos_downtime +
        pontos_mttr +
        pontos_reincidencia
    )

    if nota >= 85:
        status = "saudável"

    elif nota >= 70:
        status = "degradada"

    elif nota >= 50:
        status = "crítica"

    else:
        status = "colapso"

    return nota, status


# =========================================
# RISCO OPERACIONAL
# =========================================

def calcular_risco_operacional(
        uptime_atual,
        uptime_anterior,
        downtime_atual,
        downtime_anterior,
        mttr_atual,
        mttr_anterior,
        reincidencias
):

    score = 0

    if uptime_atual < uptime_anterior:
        score += 35

    if downtime_atual > downtime_anterior:
        score += 25

    if mttr_atual > mttr_anterior:
        score += 20

    if reincidencias >= 5:
        score += 20

    if score >= 70:
        risco = "alto"

    elif score >= 40:
        risco = "medio"

    else:
        risco = "baixo"

    return score, risco


# =========================================
# PROJEÇÃO
# =========================================

def gerar_projecao(
        serie: list
) -> list:

    if len(serie) < 2:
        return []

    diferenca = (
        serie[-1] -
        serie[-2]
    )

    ultimo = serie[-1]

    projecao = []

    for _ in range(4):

        ultimo += diferenca

        projecao.append(
            round(ultimo, 2)
        )

    return projecao


# =========================================
# JSON DASHBOARD
# =========================================

def gerar_dashboard_json(
        df: pd.DataFrame,
        periodo_nome: str,
        dias: int
):

    data_final = df["data_hora"].max()

    data_inicio = (
        data_final -
        timedelta(days=dias)
    )

    periodo_anterior_inicio = (
        data_inicio -
        timedelta(days=dias)
    )

    df_periodo = df[
        df["data_hora"] >= data_inicio
    ]

    df_periodo_anterior = df[
        (
            df["data_hora"] >=
            periodo_anterior_inicio
        ) &
        (
            df["data_hora"] < data_inicio
        )
    ]

    # =====================================
    # UPTIME
    # =====================================

    uptime_atual = calcular_uptime(
        df_periodo
    )

    uptime_anterior = calcular_uptime(
        df_periodo_anterior
    )

    # =====================================
    # DOWNTIME
    # =====================================

    downtime_atual = int(
        df_periodo[
            "minutos_downtime"
        ].sum()
    )

    downtime_anterior = int(
        df_periodo_anterior[
            "minutos_downtime"
        ].sum()
    )

    percentual_downtime = (
        downtime_atual /
        max(len(df_periodo), 1)
    )

    # =====================================
    # DISPONIBILIDADE
    # =====================================

    disponibilidade = calcular_disponibilidade(
        df_periodo
    )

    serie_real = list(
        disponibilidade.values()
    )

    serie_projecao = gerar_projecao(
        serie_real[-4:]
    )

    # =====================================
    # REINCIDÊNCIAS
    # =====================================

    reincidencias = calcular_reincidencias(
        df_periodo
    )

    total_reincidencias = sum(
        reincidencias.values()
    )

    # =====================================
    # MTTR
    # =====================================

    # MOCK TEMPORÁRIO
    mttr_atual = 48
    mttr_anterior = 54

    # =====================================
    # NOTA SAÚDE
    # =====================================

    nota_saude, status_saude = (
        calcular_nota_saude(
            uptime_atual,
            percentual_downtime,
            mttr_atual,
            total_reincidencias
        )
    )

    # =====================================
    # RISCO OPERACIONAL
    # =====================================

    risco_score, risco_nivel = (
        calcular_risco_operacional(
            uptime_atual,
            uptime_anterior,
            downtime_atual,
            downtime_anterior,
            mttr_atual,
            mttr_anterior,
            total_reincidencias
        )
    )

    # =====================================
    # ZONAS
    # =====================================

    zonas = []

    total_displays = (
        df["idDisplay"]
        .nunique()
    )

    for zona, dados in df_periodo.groupby(
            "idZona"
    ):

        downtime_zona = int(
            dados[
                "minutos_downtime"
            ].sum()
        )

        displays_afetados = (
            dados["idDisplay"]
            .nunique()
        )

        percentual_displays = round(
            (
                displays_afetados /
                total_displays
            ) * 100,
            2
        )

        eventos = dados[
            dados[
                "minutos_downtime"
            ] > 0
        ]

        reincidencia_zona = len(
            eventos
        )

        if reincidencia_zona >= 5:
            risco_zona = "alto"

        elif reincidencia_zona >= 3:
            risco_zona = "medio"

        else:
            risco_zona = "baixo"

        tendencia_real = (
            eventos.groupby(
                eventos[
                    "data_hora"
                ].dt.date
            ).size().tolist()
        )

        tendencia_projecao = (
            gerar_projecao(
                tendencia_real[-4:]
            )
        )

        zonas.append({
            "zona": f"Zona {zona}",

            "risco": risco_zona,

            "reincidencias": reincidencia_zona,

            "downtime_minutos": downtime_zona,

            "displays": {
                "afetados": displays_afetados,
                "total": total_displays,
                "percentual": percentual_displays
            },

            "tendencia": {
                "real": tendencia_real,
                "projecao": tendencia_projecao
            }
        })

    # =====================================
    # JSON FINAL
    # =====================================

    dashboard = {

        "periodo": periodo_nome,

        "kpis": {

            "saude_operacional": {
                "nota": nota_saude,
                "status": status_saude,
                "tendencia": serie_real[-4:]
            },

            "uptime": {
                "atual": uptime_atual,
                "meta": META_UPTIME,
                "anterior": uptime_anterior
            },

            "downtime": {
                "atual_minutos": downtime_atual,
                "anterior_minutos": downtime_anterior,
                "projecao_minutos": (
                    downtime_atual +
                    (
                        downtime_atual -
                        downtime_anterior
                    )
                )
            },

            "mttr": {
                "atual": mttr_atual,
                "anterior": mttr_anterior,
                "meta": META_MTTR
            },

            "risco_operacional": {
                "nivel": risco_nivel,
                "score": risco_score,
                "dias_para_violacao_sla": 4
            }
        },

        "disponibilidade": {
            "labels": list(
                disponibilidade.keys()
            ),

            "real": serie_real,

            "projecao": serie_projecao
        },

        "zonas": zonas
    }

    return dashboard


# =========================================
# EXECUÇÃO LOCAL
# =========================================

if __name__ == "__main__":

    caminho_csv = "dados.csv"

    df = carregar_csv(
        caminho_csv
    )

    df = calcular_downtime(df)

    dashboard = gerar_dashboard_json(
        df,
        "mensal",
        30
    )

    with open(
            "dashboard.json",
            "w",
            encoding="utf-8"
    ) as arquivo:

        json.dump(
            dashboard,
            arquivo,
            indent=2,
            ensure_ascii=False
        )

    print(
        "dashboard.json gerado com sucesso."
    )
