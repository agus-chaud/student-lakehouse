"""
DAG: etl_fase2_monolith.py
Ejecuta el pipeline monolítico de Fase-2 (carga directa de Parquet a PostgreSQL)
"""

import io
from datetime import datetime, timedelta
from time import time

import pandas as pd
import psycopg2
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator

# ─── CONFIGURACIÓN FIJA (equivalente al script de Fase-2) ───
PARQUET_FILE = "/opt/airflow/Proyectos/input/yellow_tripdata_2024-01.parquet"  # Ruta accesible al worker
TABLE_NAME = "yellow_taxi_data"
PG_CONFIG = {
    "host": "postgres-container",      # o "localhost" si Airflow está en el mismo cluster
    "dbname": "ny_taxi",
    "user": "postgres",
    "password": "changeme1234",          # idealmente desde Airflow Variables/Connections
}

# ─── FUNCIÓN QUE CONTIENE TODO EL PIPELINE ───
def run_etl_fase2():
    """Carga completa: crear tabla + COPY por row groups (tal cual el script de Fase-2)."""

    # 1) Crear tabla automáticamente
    engine_url = (
        f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
        f"@{PG_CONFIG['host']}:5432/{PG_CONFIG['dbname']}"
    )
    engine = create_engine(engine_url)

    # Solo se lee el esquema (metadatos del footer), no los ~3M de filas
    df_schema = pq.ParquetFile(PARQUET_FILE).schema_arrow.empty_table().to_pandas()
    df_schema.to_sql(name=TABLE_NAME, con=engine, if_exists="replace", index=False)
    print("✔ Tabla creada automáticamente (solo esquema)")

    # 2) Lectura del Parquet
    pqfile = pq.ParquetFile(PARQUET_FILE)
    num_groups = pqfile.num_row_groups
    print(f"Parquet detectado con {num_groups} row groups")

    # 3) Conexión nativa a PostgreSQL
    conn = psycopg2.connect(
        host=PG_CONFIG["host"],
        dbname=PG_CONFIG["dbname"],
        user=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
    )
    cursor = conn.cursor()

    # 4) Procesamiento por row groups
    for i in range(num_groups):
        t_start = time()
        print(f"→ Procesando row group {i + 1}/{num_groups}")

        table = pqfile.read_row_group(i)
        df = table.to_pandas()

        # Normalización de tipos (igual que en Fase-2)
        for col in df.columns:
            if df[col].dtype == "float64":
                if ((df[col] % 1 == 0) | df[col].isnull()).all():
                    df[col] = df[col].astype("Int64")
        for col in df.columns:
            if "datetime" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # COPY FROM STDIN
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV", buffer)
        conn.commit()

        t_end = time()
        print(f"✔ Row group {i + 1} cargado en {t_end - t_start:.2f} segundos")

    # 5) Cierre
    cursor.close()
    conn.close()
    print("✔ Carga completa finalizada")

# ─── DEFINICIÓN DEL DAG ───
default_args = {
    "owner": "data_team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),  # por defecto son 5 min
}

with DAG(
    dag_id="etl_fase2_monolith",
    default_args=default_args,
    schedule_interval=None,              # Solo se ejecuta manualmente (para el LAB)
    catchup=False,
    tags=["fase2", "nyc_taxi"],
    description="Pipeline monolítico de carga NYC Taxi (Fase-2) orquestado con Airflow",
) as dag:

    tarea_unica = PythonOperator(
        task_id="cargar_yellow_taxi_data",
        python_callable=run_etl_fase2,
    )
