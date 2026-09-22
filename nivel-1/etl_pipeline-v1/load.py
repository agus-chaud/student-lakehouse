import io
import logging
import re
from contextlib import contextmanager
from time import perf_counter

import psycopg2
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import pandas as pd

from utils import measure


def load_dataframe_to_postgres(cfg: dict, table: str, df: pd.DataFrame) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"Nombre de tabla inválido: {table}")
    engine = create_engine(build_connection_string(cfg))
    try:
        df.to_sql(table, engine, if_exists="replace", index=False)
    finally:
        engine.dispose()
    logging.getLogger("etl_pipeline").info(
        "Modelo Gold cargado en PostgreSQL | tabla=%s | filas=%s", table, len(df)
    )


def build_connection_string(cfg: dict) -> str:
    pg = cfg["postgres"]
    return (
        f"postgresql://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['db']}"
    )


@contextmanager
def postgres_cursor(cfg: dict):
    conn = psycopg2.connect(
        host=cfg["postgres"]["host"],
        port=cfg["postgres"]["port"],
        dbname=cfg["postgres"]["db"],
        user=cfg["postgres"]["user"],
        password=cfg["postgres"]["password"],
    )
    cursor = conn.cursor()
    try:
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()


def create_table(cfg: dict, pqfile: pq.ParquetFile) -> list[str]:
    table = cfg["postgres"]["table"]
    conn_str = build_connection_string(cfg)
    engine = create_engine(conn_str)

    df_schema = pqfile.read_row_group(0).slice(0, 0).to_pandas()
    df_schema.columns = [c.lower() for c in df_schema.columns]
    df_schema.to_sql(table, engine, if_exists="replace", index=False)
    engine.dispose()

    columns = list(df_schema.columns)
    logging.getLogger("etl_pipeline").info(f"Tabla '{table}' creada con {len(columns)} columnas.")
    return columns


def copy_to_postgres(cursor, conn, table: str, df: pd.DataFrame) -> None:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cursor.copy_expert(f"COPY {table} FROM STDIN WITH CSV", buffer)
    conn.commit()


def load_row_group(cursor, conn, cfg: dict, df: pd.DataFrame, group_index: int, total_groups: int) -> None:
    table = cfg["postgres"]["table"]
    t_start = perf_counter()
    copy_to_postgres(cursor, conn, table, df)
    elapsed = perf_counter() - t_start
    logging.getLogger("etl_pipeline").info(
        f"Row group {group_index + 1}/{total_groups} | "
        f"{len(df):,} filas | {elapsed:.2f}s"
    )


def run(cfg: dict, pqfile: pq.ParquetFile, row_groups_iter) -> dict:
    columns = create_table(cfg, pqfile)
    total_rows = 0
    total_groups = 0

    with measure("load") as m:
        with postgres_cursor(cfg) as (conn, cursor):
            for group_index, total_groups_count, df in row_groups_iter:
                load_row_group(cursor, conn, cfg, df, group_index, total_groups_count)
                total_rows += len(df)
                total_groups += 1

    m.rows_processed = total_rows
    m.extra = {"groups": total_groups, "columns": len(columns)}

    logging.getLogger("etl_pipeline").info(
        f"Carga finalizada - {total_rows:,} filas en {total_groups} grupos."
    )
    return {"total_rows": total_rows, "total_groups": total_groups}
