import dotenv
import requests
import pandas as pd
import os
from requests.auth import HTTPBasicAuth

dotenv.load_dotenv()

# E-mail do Jira
EMAIL = os.getenv('JIRA_EMAIL')

# Token do Jira
TOKEN = os.getenv('JIRA_TOKEN')

if not EMAIL:
    raise ValueError("JIRA_EMAIL não configurado")

if not TOKEN:
    raise ValueError("JIRA_TOKEN não configurado")

# URL da API do Jira
url = "https://vooh.atlassian.net/rest/api/3/search/jql"

# Parâmetros da requisição
params = {
    "jql": "project = SCRUM",
    "maxResults": 100,
    "fields": "summary,status,created,resolutiondate,priority,assignee"
}

# Resposta da requisição
response = requests.get(
    url,
    auth=HTTPBasicAuth(EMAIL, TOKEN),
    params=params,
    headers={
        "Accept": "application/json"
    }
)

# Caso a requisição tenha sucesso (status code 200)
if response.status_code == 200:
    dados_jira = response.json()
    print("Chaves retornadas:", dados_jira.keys())

    if "issues" not in dados_jira:
        print("Nenhuma issue encontrada.")
        print(dados_jira)
        exit()

    dados = []

    for issue in dados_jira["issues"]:
        fields = issue.get("fields", {})

        dados.append({
            "ticket": issue.get("key"),
            "titulo": fields.get("summary"),

            "status":
                fields.get("status", {}).get("name")
                if fields.get("status")
                else None,

            "prioridade":
                fields.get("priority", {}).get("name")
                if fields.get("priority")
                else None,

            "criado": fields.get("created"),
            "resolvido": fields.get("resolutiondate")
        })

    jira = pd.DataFrame(dados)
    if jira.empty:
        print("DataFrame vazio.")
        exit()

    print(jira.head())

    abertos = jira["resolvido"].isna().sum()
    fechados = jira["resolvido"].notna().sum()

    jira["criado"] = pd.to_datetime(jira["criado"], errors="coerce")

    jira["resolvido"] = pd.to_datetime(jira["resolvido"], errors="coerce")

    resolvidos = jira[jira["resolvido"].notna()].copy()

    if len(resolvidos) > 0:
        resolvidos["mttr_min"] = (resolvidos["resolvido"] - resolvidos["criado"]).dt.total_seconds() / 60
        mttr_medio = resolvidos["mttr_min"].mean()
        melhor_mttr = resolvidos["mttr_min"].min()
        pior_mttr = resolvidos["mttr_min"].max()

        resultado = {
            "abertos": int(abertos),
            "fechados": int(fechados),
            "mttr_medio": round(float(mttr_medio), 2),
            "melhor_mttr": round(float(melhor_mttr), 2),
            "pior_mttr": round(float(pior_mttr), 2)
        }

        print(resultado)
    else:
        mttr_medio = 0
        melhor_mttr = 0
        pior_mttr = 0

# Caso não seja status code 200
else:
    print(response.text)
    exit(1)