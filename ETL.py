import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta
import time
import random
from dotenv import load_dotenv

# 1. CARREGAR VARIÁVEIS DO .ENV

load_dotenv()  

user = os.getenv("DB_USER")
password = os.getenv("DB_PASS")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")



engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}/{database}",
    echo=False
)


# CONFIGURAÇÃO GERAL

os.makedirs("dados", exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

SERIES = {
    "IPCA": 433,
    "SELIC": 432,
    "USD_BRL": 10813
}

MAX_YEARS = 9


# FUNÇÃO RETRY

def request_retry(url, params, retries=5):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=20)
            r.raise_for_status()
            return r.json()

        except Exception as e:
            wait = 1 + attempt * 2 + random.random()
            print(f"    ⚠ FALHA (TENTATIVA {attempt}/{retries}): {e}")
            print(f"    ⏳ AGUARDANDO {wait:.1f}s PARA TENTAR NOVAMENTE...")
            time.sleep(wait)

    raise Exception(f"FALHA PERMANENTE APÓS {retries} TENTATIVAS.")

# FUNÇÕES DE COLETA E TRATAMENTO


def daterange_chunks(start_date, end_date, years_chunk):
    cur_start = start_date
    while cur_start <= end_date:
        try:
            cur_end = cur_start.replace(year=cur_start.year + years_chunk)
        except:
            cur_end = end_date

        if cur_end > end_date:
            cur_end = end_date

        yield cur_start, cur_end
        cur_start = cur_end + timedelta(days=1)


def fetch_sgs_series_block(codigo, dt_start, dt_end):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
    params = {
        "formato": "json",
        "dataInicial": dt_start.strftime("%d/%m/%Y"),
        "dataFinal": dt_end.strftime("%d/%m/%Y")
    }
    return request_retry(url, params)


def fetch_series_concat(codigo, start, end):
    sd = datetime.fromisoformat(start)
    ed = datetime.fromisoformat(end)
    all_records = []

    print(f"\n BUSCANDO SÉRIE {codigo} ({start} → {end})")

    for block_start, block_end in daterange_chunks(sd, ed, MAX_YEARS):
        print(f"  → Bloco {block_start.date()} → {block_end.date()}")
        try:
            js = fetch_sgs_series_block(codigo, block_start, block_end)
            print(f"     ✓ {len(js)} REGISTROS RECEBIDOS")
            all_records.extend(js)
        except Exception as e:
            print(f"      ERRO NO BLOCO: {e}")

    df = pd.DataFrame(all_records)

    if df.empty:
        return df

    df.rename(columns={"data": "data", "valor": "valor"}, inplace=True)

    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
    df["valor"] = (
        df["valor"].astype(str).str.replace(",", ".").str.strip()
    )
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    df.dropna(subset=["data"], inplace=True)
    df.sort_values("data", inplace=True)

    return df.reset_index(drop=True)[["data", "valor"]]

# SALVAR NO MySQL

def save_to_mysql_and_csv(df, name):
    if df.empty:
        print(f"NADA PARA SALVAR EM {name}.")
        return

    csv_path = os.path.join("dados", f"{name.lower()}.csv")
    df.to_csv(csv_path, index=False)

    df_db = df.copy()
    df_db.columns = ["date", "value"]
    df_db.to_sql(name.lower(), engine, if_exists="replace", index=False)

    print(f" {name} SALVO NO MySQL")
