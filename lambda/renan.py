import os
import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

# ── Cliente S3 ───────────────────────────────────────────────────────────────
s3     = boto3.client('s3')  
BUCKET = os.environ['AWS_BUCKET_NAME']

# s3     = boto3.client(
#     's3',
#     aws_access_key_id     = os.getenv('AWS_ACCESS_KEY_ID'),
#     aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
#     aws_session_token     = os.getenv('AWS_SESSION_TOKEN')
# )

# BUCKET = os.getenv('AWS_BUCKET_NAME')
hoje   = datetime.now().strftime('%d_%m_%Y')

def formatarVelocidade(bps):
    if bps >= 1_073_741_824:
        return f"{bps / 1_073_741_824:.2f} GB/s"
    if bps >= 1_048_576:
        return f"{bps / 1_048_576:.2f} MB/s"
    if bps >= 1024:
        return f"{bps / 1024:.1f} KB/s"
    return f"{bps:.0f} B/s"


def lambda_handler(event, context):  
    hoje = datetime.now().strftime('%d_%m_%Y')

    # ── Lê CSV do S3 ─────────────────────────────────────────────────────────
    resposta = s3.list_objects_v2(Bucket=BUCKET, Prefix='trusted/')
    csvDados = None

    for item in resposta.get('Contents', []):
        chave = item['Key']
        nome  = os.path.basename(chave)
        if hoje not in nome or not nome.endswith('.csv'):
            continue
        print(f"Lendo: s3://{BUCKET}/{chave}")
        obj      = s3.get_object(Bucket=BUCKET, Key=chave)
        conteudo = obj['Body'].read().decode('utf-8')
        csvDados = pd.read_csv(StringIO(conteudo))
        break

    if csvDados is None:
        raise FileNotFoundError(f"Nenhum CSV de hoje ({hoje}) encontrado em trusted/")

    # ── Preparo ───────────────────────────────────────────────────────────────
    csvDados['data_hora'] = pd.to_datetime(csvDados['data_hora'], format='mixed', dayfirst=True)
    csvDados = csvDados.sort_values(['idDisplay', 'data_hora']).reset_index(drop=True)
    csvDados = csvDados.drop_duplicates(subset=['idDisplay', 'data_hora']).reset_index(drop=True)

    csvDados['boot_dia_hora'] = csvDados['boot_time'].apply(
        lambda x: ','.join(str(x).split(',')[:2])
    )
    csvDados['boot_mudou'] = csvDados.groupby('idDisplay')['boot_dia_hora'].transform(
        lambda s: s != s.shift(1)
    )

    # ── Delta de rede ────────────────────────────────────────────
    resultados = []
    for _, grupo in csvDados.groupby('idDisplay'):
        grupo = grupo.copy()
        dt = grupo['data_hora'].diff().dt.total_seconds()
        grupo['download_bps'] = grupo['download_mb'].div(dt).clip(lower=0)
        grupo['upload_bps']   = grupo['upload_mb'].div(dt).clip(lower=0)
        grupo['dropin_delta'] = grupo['dropin'].diff().clip(lower=0)
        grupo['errin_delta']  = grupo['errin'].diff().clip(lower=0)
        resultados.append(grupo)

    csvDados = pd.concat(resultados).reset_index(drop=True)

    for col in ['download_bps', 'upload_bps', 'dropin_delta', 'errin_delta']:
        csvDados.loc[csvDados['boot_mudou'], col] = 0

    csvDados = csvDados.dropna(subset=['download_bps', 'upload_bps']).reset_index(drop=True)
    csvDados['download_mbs'] = csvDados['download_bps'].round(4)
    csvDados['upload_mbs']   = csvDados['upload_bps'].round(4)

    # ── Geração de JSON  ──────────────────────────────────
    for idEmpresa in csvDados['idEmpresa'].unique():
        dfEmpresa = csvDados[csvDados['idEmpresa'] == idEmpresa].copy()
        novoJson  = {}

        for idDisplay in dfEmpresa['idDisplay'].unique():
            dfDisplay = dfEmpresa[dfEmpresa['idDisplay'] == idDisplay].copy()

            ultimoRegistro = dfDisplay['data_hora'].max()
            dfJanela = dfDisplay[
                dfDisplay['data_hora'] >= ultimoRegistro - pd.Timedelta(minutes=60)
            ].copy()

            ultimo = dfDisplay.iloc[-1]
            mtu    = int(ultimo['mtu'])

            registrosEsperados = 24 * 60
            percentual = round((len(dfDisplay) / registrosEsperados) * 100, 1)
            percentual = min(percentual, 100.0)

            if percentual >= 99:   statusDisp = "Excelente"
            elif percentual >= 95: statusDisp = "Normal"
            elif percentual >= 90: statusDisp = "Degradado"
            else:                  statusDisp = "Crítico"

            dfDisplay = dfDisplay.copy()
            dfDisplay['hora'] = dfDisplay['data_hora'].dt.floor('h')

            estabilidadeHoras = []
            for hora, grupo in dfDisplay.groupby('hora'):
                estabilidadeHoras.append({
                    "hora":          hora.strftime('%H:00'),
                    "uptime":        round(min((len(grupo) / 60) * 100, 100), 1),
                    "latenciaMedia": round(float(grupo['latencia'].mean()), 1),
                    "perdaPacotes":  int(grupo['dropin_delta'].sum())
                })

            fluxoDados = [
                {
                    "timestamp":    row['data_hora'].strftime('%H:%M:%S'),
                    "download_mbs": round(float(row['download_mbs']), 4),
                    "upload_mbs":   round(float(row['upload_mbs']),   4)
                }
                for _, row in dfJanela.iterrows()
            ]

            latenciaHistorico = [
                {
                    "timestamp": row['data_hora'].strftime('%H:%M:%S'),
                    "latencia":  round(float(row['latencia']), 1)
                }
                for _, row in dfJanela.iterrows()
            ]

            kpis = {
                "download_mbs":        round(float(ultimo['download_mbs']), 4),
                "download_formatado":  formatarVelocidade(float(ultimo['download_mbs']) * 1_048_576),
                "upload_mbs":          round(float(ultimo['upload_mbs']), 4),
                "upload_formatado":    formatarVelocidade(float(ultimo['upload_mbs']) * 1_048_576),
                "pacotes_descartados": int(ultimo['dropin_delta']),
                "erros_io":            int(ultimo['errin_delta']),
                "latencia_ms":         round(float(ultimo['latencia']), 1)
            }

            novoJson[str(idDisplay)] = {
                "idDisplay":         int(idDisplay),
                "idEmpresa":         int(idEmpresa),
                "mac":               str(ultimo['mac']),
                "ip":                str(ultimo['ip']),
                "dataHora":          ultimoRegistro.strftime('%d-%m-%Y %H:%M:%S'),
                "kpis":              kpis,
                "fluxoDados":        fluxoDados,
                "latenciaHistorico": latenciaHistorico,
                "conexoes": {
                    "established": int(ultimo['conn_established']),
                    "listen":      int(ultimo['conn_listen']),
                    "time_wait":   int(ultimo['conn_time_wait']),
                    "close_wait":  int(ultimo['conn_close_wait']),
                    "syn_sent":    int(ultimo['conn_syn_sent'])
                },
                "mtu": {
                    "valor": mtu,
                    "tipo":  "loopback" if mtu == 65536 else "jumbo frame" if mtu == 9000 else "padrão"
                },
                "disponibilidade": {"percentual": percentual, "status": statusDisp},
                "estabilidade":    estabilidadeHoras,
                "diagnostico": {
                    "download_ativo":      float(ultimo['download_mbs']) > 0,
                    "pacotes_descartados": int(ultimo['dropin_delta']),
                    "erros_pacote":        int(ultimo['errin_delta']),
                    "latencia_ok":         float(ultimo['latencia']) < 100,
                    "mtu_padrao":          mtu == 1500
                }
            }

        nomeArquivo = f'client/json_Rede{idEmpresa}_{hoje}.json'
        s3.put_object(
            Bucket      = BUCKET,
            Key         = nomeArquivo,
            Body        = json.dumps(novoJson, ensure_ascii=False, indent=4).encode('utf-8'),
            ContentType = 'application/json'
        )
        print(f"Empresa {idEmpresa} → s3://{BUCKET}/{nomeArquivo}")
        print(csvDados[['idDisplay', 'data_hora', 'download_mb', 'download_bps', 'download_mbs']].head(20).to_string())

    return {"statusCode": 200, "body": "Processamento concluído"}


    