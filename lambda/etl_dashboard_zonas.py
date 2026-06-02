import pandas as pd
import json
import boto3
import os
import mysql.connector
from datetime import datetime, timedelta
from io import BytesIO
from zoneinfo import ZoneInfo

BUCKET          = 's3-grupovooh-raw'
PREFIXO_TRUSTED = 'trusted/'
RETENCAO_DIAS   = 7

_agora    = datetime.now(ZoneInfo('America/Sao_Paulo'))
hoje      = _agora.strftime('%d-%m-%Y')
chave_dia = _agora.strftime('%d-%m')


s3 = boto3.client(
    's3',
    aws_access_key_id     = os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
    aws_session_token     = os.environ.get('AWS_SESSION_TOKEN'),
    region_name           = os.environ.get('AWS_REGION', 'us-east-1'),
)


def lerCsv(prefixo_arquivo: str) -> pd.DataFrame:
    resposta = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIXO_TRUSTED)
    arquivos = [
        item['Key']
        for item in resposta.get('Contents', [])
        if os.path.basename(item['Key']).startswith(prefixo_arquivo)
        and hoje.replace('-', '_') in item['Key']
    ]
    frames = []
    for key in arquivos:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        df  = pd.read_csv(BytesIO(obj['Body'].read()), encoding='utf-8', sep=",")
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def lerJson(idEmpresa: int) -> dict:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f'client/dashZonas_Empresa{idEmpresa}.json')
        arquivo = obj['Body'].read().decode('utf-8')
        return json.loads(arquivo)
    except Exception:
        return {"dias": {}, "historico7dias": {}}


def enviarJson(idEmpresa: int, payload: dict) -> None:
    s3.put_object(
        Bucket      = BUCKET,
        Key         = f'client/dashZonas_Empresa{idEmpresa}.json',
        Body        = json.dumps(payload, ensure_ascii=False, indent=4),
        ContentType = 'application/json',
    )


def aplicarRetencao(dias: dict) -> dict:
    if len(dias) <= RETENCAO_DIAS:
        return dias

    chaves_ordenadas    = sorted(dias.keys(), key=lambda d: datetime.strptime(d, '%d-%m-%Y'))
    chaves_para_remover = chaves_ordenadas[:len(chaves_ordenadas) - RETENCAO_DIAS]
    for chave in chaves_para_remover:
        del dias[chave]

    return dias


def calcularAlertas(df_zona: pd.DataFrame, thresholds: dict) -> dict:
    mapa = {
        "cpu":      "cpu_percentual",
        "ram":      "ram_percentual",
        "disco":    "disco_percentual",
        "latencia": "latencia",
    }
    alertas = {k: 0 for k in mapa}
    alertas["total"] = 0

    for chave, campo in mapa.items():
        if campo not in df_zona.columns or chave not in thresholds:
            continue

        limite = thresholds[chave].get("max")
        if limite is None:
            continue

        df_ord = df_zona.sort_values(['idDisplay', 'data_hora'])

        eventos = 0
        for _, df_disp in df_ord.groupby('idDisplay'):
            violando = False
            for valor in df_disp[campo]:
                if valor > limite:
                    if not violando:
                        eventos += 1
                        violando = True
                else:
                    violando = False

        alertas[chave] = eventos
        alertas["total"] += eventos

    return alertas

INTERVALO_NORMAL_MIN = 1.5
def calcularDisponibilidade(df_display: pd.DataFrame) -> float:
    if df_display.empty or len(df_display) < 2:
        return 100.0

    df = df_display.copy().sort_values('data_hora')
    df['dt'] = pd.to_datetime(df['data_hora'], dayfirst=True)

    tempo_total = (df['dt'].iloc[-1] - df['dt'].iloc[0]).total_seconds() / 60
    if tempo_total <= 0:
        return 100.0

    df['intervalo_min'] = df['dt'].diff().dt.total_seconds() / 60

    gaps = df['intervalo_min'][df['intervalo_min'] > INTERVALO_NORMAL_MIN]
    tempo_offline = (gaps - INTERVALO_NORMAL_MIN).sum()

    return round(min((tempo_total - tempo_offline) / tempo_total * 100, 100), 2)


def calcularHistorico24hDisplay(df_display: pd.DataFrame) -> dict:

    if df_display.empty or len(df_display) < 2:
        return {}

    df = df_display.copy().sort_values('data_hora')
    df['dt']            = pd.to_datetime(df['data_hora'], dayfirst=True)
    df['hora']          = df['dt'].dt.strftime('%Hh')
    df['intervalo_min'] = df['dt'].diff().dt.total_seconds() / 60

    historico = {}
    for hora, grupo in df.groupby('hora'):
        if len(grupo) < 2:
            historico[hora] = 100.0
            continue

        tempo_total = (grupo['dt'].iloc[-1] - grupo['dt'].iloc[0]).total_seconds() / 60
        if tempo_total <= 0:
            historico[hora] = 100.0
            continue

        gaps = grupo['intervalo_min'][grupo['intervalo_min'] > INTERVALO_NORMAL_MIN]
        tempo_offline   = (gaps - INTERVALO_NORMAL_MIN).sum()
        historico[hora] = round(min((tempo_total - tempo_offline) / tempo_total * 100, 100), 2)

    return historico


def calcularHistorico24hZona(df_zona: pd.DataFrame) -> dict:

    historicos_displays = {}
    for idDisplay in df_zona['idDisplay'].unique():
        df_disp = df_zona[df_zona['idDisplay'] == idDisplay]
        historicos_displays[idDisplay] = calcularHistorico24hDisplay(df_disp)

    todas_horas = set()
    for hist in historicos_displays.values():
        todas_horas.update(hist.keys())

    historico_zona = {}
    for hora in sorted(todas_horas):
        vals = [
            hist[hora]
            for hist in historicos_displays.values()
            if hora in hist
        ]
        historico_zona[hora] = round(sum(vals) / len(vals), 2) if vals else 100.0

    return historico_zona


def processar(
    df: pd.DataFrame,
    thresholds: dict,
    meta_por_empresa: dict,
    nomes_zonas: dict,
) -> None:
    for idEmpresa in df['idEmpresa'].unique():
        df_empresa = df[df['idEmpresa'] == idEmpresa].copy()
        meta       = meta_por_empresa.get(int(idEmpresa), 95.0)

        zonas_resultado = {}
        uptime_por_zona = {}
        historico24h    = {} 

        for idZona in df_empresa['idZona'].unique():
            df_zona  = df_empresa[df_empresa['idZona'] == idZona]
            nomeZona = nomes_zonas.get(int(idZona), f'Zona {idZona}')

            zona_alertas = calcularAlertas(df_zona, thresholds)

            uptimes_displays = [
                calcularDisponibilidade(df_zona[df_zona['idDisplay'] == idDisplay])
                for idDisplay in df_zona['idDisplay'].unique()
            ]
            uptime_zona = round(sum(uptimes_displays) / len(uptimes_displays), 2) if uptimes_displays else 0.0

            historico24h[nomeZona] = calcularHistorico24hZona(df_zona)

            if uptime_zona >= meta + 3:
                status_zona = "Estável"
            elif uptime_zona >= meta:
                status_zona = "Em Risco"
            else:
                status_zona = "Crítico"

            zonas_resultado[str(idZona)] = {
                "idZona":              int(idZona),
                "nomeZona":            nomeZona,
                "totalDisplays":       df_zona['idDisplay'].nunique(),
                "uptime":              uptime_zona,
                "metaDisponibilidade": meta,
                "status":              status_zona,
                "alertas":             zona_alertas,
            }
            uptime_por_zona[str(idZona)] = uptime_zona

        total_displays   = sum(z["totalDisplays"] for z in zonas_resultado.values())
        uptime_geral     = round(sum(uptime_por_zona.values()) / len(uptime_por_zona), 2) if uptime_por_zona else 0.0
        zonas_com_alerta = sum(1 for z in zonas_resultado.values() if z["alertas"]["total"] > 0)

        zona_critica_id = max(zonas_resultado, key=lambda z: zonas_resultado[z]["alertas"]["total"], default=None)
        zona_estavel_id = min(zonas_resultado, key=lambda z: zonas_resultado[z]["alertas"]["total"], default=None)

        zona_critica = {
            "idZona":   zonas_resultado[zona_critica_id]["idZona"],
            "nomeZona": zonas_resultado[zona_critica_id]["nomeZona"],
        } if zona_critica_id else None

        zona_estavel = {
            "idZona":   zonas_resultado[zona_estavel_id]["idZona"],
            "nomeZona": zonas_resultado[zona_estavel_id]["nomeZona"],
        } if zona_estavel_id else None

        dadosJson      = lerJson(idEmpresa)
        dias           = dadosJson.get('dias', {})
        historico7dias = dadosJson.get('historico7dias', {})

        for zona in zonas_resultado.values():
            nome = zona["nomeZona"]
            if nome not in historico7dias:
                historico7dias[nome] = {}
            historico7dias[nome][chave_dia] = zona["uptime"]

            if len(historico7dias[nome]) > RETENCAO_DIAS:
                chaves_ord = sorted(
                    historico7dias[nome].keys(),
                    key=lambda d: datetime.strptime(d + f'-{datetime.now().year}', '%d-%m-%Y')
                )
                for c in chaves_ord[:len(chaves_ord) - RETENCAO_DIAS]:
                    del historico7dias[nome][c]

        dias[hoje] = {
            "data":      hoje,
            "idEmpresa": int(idEmpresa),
            "kpis": {
                "totalDisplays":  total_displays,
                "totalZonas":     len(zonas_resultado),
                "zonasComAlerta": zonas_com_alerta,
                "uptimeGeral":    uptime_geral,
                "zonaCritica":    zona_critica,
                "zonaEstavel":    zona_estavel,
            },
            "historico24h": historico24h,
            "zonas":        zonas_resultado, 
        }

        dias = aplicarRetencao(dias)

        enviarJson(idEmpresa, {
            "dias":          dias,
            "historico7dias": historico7dias,
        })
        print(f"✅ Empresa {idEmpresa} | Uptime: {uptime_geral}% | Zonas com alerta: {zonas_com_alerta}/{len(zonas_resultado)}")



def handler(event, context):
    df = lerCsv('dados_empresa')

    conexao = mysql.connector.connect(
        host     = os.environ['DBHOST'],
        user     = os.environ['DBUSER'],
        password = os.environ['DBPASSWORD'],
        database = os.environ['DBDATABASE'],
    )
    cursor = conexao.cursor(dictionary=True)

    cursor.execute("""
SELECT
    fkEmpresa,
    metaDisponibilidade
FROM contrato
WHERE periodo_inicial <= NOW()
  AND periodo_final   >= NOW()
ORDER BY ultima_referencia DESC
""")
    rows_meta = cursor.fetchall()

    cursor.execute("SELECT idZona, nome FROM zona")
    rows_zonas = cursor.fetchall()

    cursor.close()
    conexao.close()

    thresholds_por_empresa: dict = {
        "cpu":      {"max": 50},
        "ram":      {"max": 50},
        "disco":    {"max": 50},
        "latencia": {"max": 20},
    }

    meta_por_empresa: dict = {}
    for row in rows_meta:
        emp = row['fkEmpresa']
        if emp not in meta_por_empresa:
            meta_por_empresa[emp] = float(row['metaDisponibilidade'])

    nomes_zonas: dict = {row['idZona']: row['nome'] for row in rows_zonas}

    processar(df, thresholds_por_empresa, meta_por_empresa, nomes_zonas)

    return {"statusCode": 200, "body": "ETL executada com sucesso"}