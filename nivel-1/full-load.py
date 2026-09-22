from pathlib import Path
from time import time
import io

import pandas as pd
import psycopg2
import pyarrow.parquet as pq
from sqlalchemy import create_engine


BASE_DIR = Path(__file__).resolve().parent
PARQUET_FILE = BASE_DIR / "data" / "yellow_tripdata_2024-01.parquet"
TABLE_NAME = "yellow_taxi_data"
DATABASE_URL = "postgresql://postgres:changeme1234@localhost:5432/ny_taxi"


def main():
    if not PARQUET_FILE.exists():
        raise FileNotFoundError(f"No existe el archivo: {PARQUET_FILE}")

    parquet_file = pq.ParquetFile(PARQUET_FILE)
    expected_rows = parquet_file.metadata.num_rows
    row_groups = parquet_file.num_row_groups

    print(f"Parquet: {PARQUET_FILE}")
    print(f"Filas esperadas: {expected_rows}")
    print(f"Row groups: {row_groups}\n")

    engine = create_engine(DATABASE_URL)

    schema = parquet_file.read_row_group(0).slice(0, 0).to_pandas()
    schema.to_sql(
        name=TABLE_NAME,
        con=engine,
        if_exists="replace",
        index=False,
    )
    engine.dispose()
    print("Tabla creada con el esquema del Parquet.\n")

    conn = psycopg2.connect(DATABASE_URL)
    total_loaded = 0

    try:
        with conn.cursor() as cursor:
            for group_number in range(row_groups):
                started_at = time()
                table = parquet_file.read_row_group(group_number)
                dataframe = table.to_pandas()

                for column in dataframe.columns:
                    if "datetime" in column.lower():
                        dataframe[column] = pd.to_datetime(
                            dataframe[column], errors="coerce"
                        )

                buffer = io.StringIO()
                dataframe.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                cursor.copy_expert(
                    f"COPY public.{TABLE_NAME} FROM STDIN WITH CSV",
                    buffer,
                )
                conn.commit()

                total_loaded += len(dataframe)
                elapsed = time() - started_at
                print(
                    f"Row group {group_number + 1}/{row_groups}: "
                    f"{len(dataframe)} filas | "
                    f"acumulado: {total_loaded} | "
                    f"{elapsed:.2f} s"
                )
    finally:
        conn.close()

    if total_loaded != expected_rows:
        raise RuntimeError(
            f"Carga incompleta: se cargaron {total_loaded} filas, "
            f"pero se esperaban {expected_rows}"
        )

    print(f"\nCarga completa finalizada: {total_loaded} filas.")


if __name__ == "__main__":
    main()