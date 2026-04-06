import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

DB_USER = os.getenv("POSTGRES_USER", "root")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")
DB_HOST = os.getenv("POSTGRES_HOST", "warehouse")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "source")

DATA_PATH = os.getenv("DATA_PATH", "./data/users.csv")
TABLE_NAME = os.getenv("TABLE_NAME", "users")


def ingest_csv_to_postgres() -> None:
    csv_path = Path(DATA_PATH)
    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo CSV: {csv_path}")

    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    print(f"Leyendo archivo: {csv_path}")
    df = pd.read_csv(csv_path)

    print(f"Filas leídas: {len(df)}")
    print(f"Cargando datos en tabla: {TABLE_NAME}")

    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

    print("Carga completada correctamente")


if __name__ == "__main__":
    ingest_csv_to_postgres()
