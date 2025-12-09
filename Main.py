from datetime import datetime
from ETL import SERIES, fetch_series_concat, save_to_mysql_and_csv


def main():
    print("\n INICIANDO ETL COM RETRY AUTOMÁTICO\n")

    start = "2000-01-01"
    end = datetime.today().strftime("%Y-%m-%d")

    for nome, codigo in SERIES.items():
        try:
            df = fetch_series_concat(codigo, start, end)
            save_to_mysql_and_csv(df, nome)
        except Exception as e:
            print(f"ERRO AO PROCESSAR {nome}: {e}")

    print("\n ETL COMPLETO! \n")

if __name__ == "__main__":
    main()
