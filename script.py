import os
import requests
import pandas as pd
from datetime import date
import calendar

print("Iniciando automação E-Agendas...")

# ===============================
# Token via Secret
# ===============================
TOKEN = os.getenv("EAGENDAS_TOKEN")
if not TOKEN:
    raise ValueError("Token não encontrado. Configure o Secret EAGENDAS_TOKEN.")

BASE_URL = "https://eagendas.cgu.gov.br/api/v2"

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

# ===============================
# Buscar agentes públicos obrigatórios
# ===============================
url_servidores = f"{BASE_URL}/agentes-publicos-obrigados?orgao_id=1190"
response = requests.get(url_servidores, headers=headers)
response.raise_for_status()

dados_servidores = response.json()
lista_servidores = dados_servidores["resposta"]["agentes_publicos_obrigados"]

df_servidores = pd.DataFrame(lista_servidores)
df_servidores_ativos = df_servidores[df_servidores["situacao"] == "Ativo"]

print(f"Servidores ativos: {len(df_servidores_ativos)}")

# ===============================
# Período: últimos 6 meses
# ===============================
hoje = date.today()
ultimo_dia_mes = calendar.monthrange(hoje.year, hoje.month)[1]
data_fim = date(hoje.year, hoje.month, ultimo_dia_mes)

mes_6_atras = hoje.month - 6
ano = hoje.year

if mes_6_atras <= 0:
    mes_6_atras += 12
    ano -= 1

data_inicio = date(ano, mes_6_atras, 1)

data_inicio_fmt = data_inicio.strftime("%d-%m-%Y")
data_fim_fmt = data_fim.strftime("%d-%m-%Y")

# ===============================
# Buscar compromissos
# ===============================
todos_compromissos = []

for _, servidor in df_servidores_ativos.iterrows():
    apo_id = servidor["apo_id"]

    params = {
        "apo_id": apo_id,
        "data_inicio": data_inicio_fmt,
        "data_termino": data_fim_fmt
    }

    resp = requests.get(
        f"{BASE_URL}/compromissos",
        headers=headers,
        params=params
    )

    if resp.status_code != 200:
        continue

    dados = resp.json()
    if not dados.get("sucesso"):
        continue

    compromissos = dados["resposta"].get("compromissos", [])
    todos_compromissos.extend(compromissos)

print(f"Compromissos coletados: {len(todos_compromissos)}")

# ===============================
# Tratamento dos dados
# ===============================
df = pd.DataFrame(todos_compromissos)
df = df.drop_duplicates(subset=["id"])

df = df.drop(
    columns=["objetivos_compromisso", "participantes_privados"],
    errors="ignore"
)

df = df.explode("participantes_publicos", ignore_index=True)

df_part = (
    pd.json_normalize(df["participantes_publicos"])
    .fillna("")
)
df_part.index = df.index

df_final = pd.concat(
    [df.drop(columns=["participantes_publicos"]), df_part],
    axis=1
)

# ===============================
# Salvar arquivo
# ===============================
os.makedirs("output", exist_ok=True)
arquivo = "output/Tb_Compromissos.xlsx"

df_final.to_excel(arquivo, index=False)

print("Arquivo gerado com sucesso:", arquivo)
