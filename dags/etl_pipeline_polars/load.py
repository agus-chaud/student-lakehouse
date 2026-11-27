import logging
import re

import polars as pl
from sqlalchemy import create_engine, inspect, text


def build_connection_string(cfg: dict) -> str:
    pg = cfg["postgres"]
    return (
        f"postgresql://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['db']}"
    )


def load_dataframe_to_postgres(cfg: dict, table: str, df: pl.DataFrame) -> None:
    """Crea/reemplaza la tabla en la base `postgres` con el contenido del DataFrame."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"Nombre de tabla inválido: {table}")
    engine = create_engine(build_connection_string(cfg))
    try:
        # Los modelos Gold son pequeños: pasar a pandas solo para to_sql es barato.
        df.to_pandas().to_sql(table, engine, if_exists="replace", index=False)
    finally:
        engine.dispose()
    logging.getLogger("etl_pipeline").info(
        "Modelo Gold cargado en PostgreSQL | tabla=%s | filas=%s", table, df.height
    )


def load_period_to_postgres(cfg: dict, table: str, df: pl.DataFrame, year: int, month: int) -> None:
    """Reemplaza solo el período (año, mes) en la tabla, sin tocar los demás.

    Agrega las columnas `year` y `month`, borra las filas de ese período e inserta
    las nuevas en una sola transacción: si algo falla, no queda el período a medias.
    Repetir la carga del mismo período da el mismo resultado (idempotente).
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"Nombre de tabla inválido: {table}")
    frame = df.with_columns(pl.lit(year).cast(pl.Int16).alias("year"), pl.lit(month).cast(pl.Int8).alias("month"))
    engine = create_engine(build_connection_string(cfg))
    try:
        with engine.begin() as conn:
            insp = inspect(conn)
            if insp.has_table(table):
                columns = {c["name"] for c in insp.get_columns(table)}
                if not {"year", "month"} <= columns:
                    raise RuntimeError(
                        f"La tabla {table} no tiene columnas year/month (esquema anterior). "
                        f"Bórrala una vez (DROP TABLE {table}) y vuelve a correr."
                    )
                deleted = conn.execute(
                    text(f"DELETE FROM {table} WHERE year = :year AND month = :month"),
                    {"year": year, "month": month},
                ).rowcount
            else:
                deleted = 0
            # Los modelos Gold son pequeños: pasar a pandas solo para to_sql es barato.
            frame.to_pandas().to_sql(table, conn, if_exists="append", index=False)
    finally:
        engine.dispose()
    logging.getLogger("etl_pipeline").info(
        "Modelo Gold cargado en PostgreSQL | tabla=%s | período=%04d-%02d | filas=%s | reemplazadas=%s",
        table, year, month, frame.height, deleted,
    )
