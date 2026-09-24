"""Limpieza, validación y enriquecimiento de la capa Silver (Polars)."""

import logging

import polars as pl


logger = logging.getLogger("etl_pipeline.silver.transform")

PAYMENT_TYPE_MAP = {
    1: "credit_card",
    2: "cash",
    3: "no_charge",
    4: "dispute",
}

RATECODE_MAP = {
    1: "standard",
    2: "jfk",
    3: "newark",
    4: "nassau",
    5: "negotiated",
    6: "group_ride",
}


def filter_invalid_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Elimina viajes con métricas, fechas o duración inválidas.

    En Polars, comparar contra null da null y filter lo descarta,
    así que no hace falta chequear is_not_null por separado.
    """
    before = df.height
    result = df.filter(
        (pl.col("passenger_count") > 0)
        & (pl.col("trip_distance") > 0)
        & (pl.col("fare_amount") >= 0)
        & (pl.col("tpep_dropoff_datetime") > pl.col("tpep_pickup_datetime"))
    )
    logger.info(
        "Filtro de calidad: %s → %s filas (%s descartadas)",
        f"{before:,}",
        f"{result.height:,}",
        f"{before - result.height:,}",
    )
    return result


def add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula duración, velocidad, atributos temporales y porcentaje de propina."""
    pickup = pl.col("tpep_pickup_datetime")
    duration = (pl.col("tpep_dropoff_datetime") - pickup).dt.total_seconds() / 60
    # Polars: lunes=1..domingo=7 → se ajusta a lunes=0..domingo=6 (igual que pandas).
    weekday = pickup.dt.weekday() - 1
    return df.with_columns(
        duration.round(2).alias("trip_duration_minutes"),
        pl.when(duration > 0)
        .then(pl.col("trip_distance") / (duration / 60))
        .alias("speed_mph"),
        pickup.dt.hour().alias("pickup_hour"),
        weekday.alias("pickup_day_of_week"),
        weekday.is_in([5, 6]).alias("is_weekend"),
        pl.when(pl.col("fare_amount") > 0)
        .then(pl.col("tip_amount") / pl.col("fare_amount") * 100)
        .round(2)
        .alias("tip_percentage"),
    )


def standardize_categoricals(df: pl.DataFrame) -> pl.DataFrame:
    """Convierte códigos TLC a etiquetas y flags Y/N a booleanos."""
    return df.with_columns(
        pl.col("payment_type").replace_strict(PAYMENT_TYPE_MAP, default=None, return_dtype=pl.String),
        pl.col("ratecodeid").replace_strict(RATECODE_MAP, default=None, return_dtype=pl.String),
        pl.col("store_and_fwd_flag").replace_strict(
            {"Y": True, "N": False}, default=None, return_dtype=pl.Boolean
        ),
    )


def run(df: pl.DataFrame) -> pl.DataFrame:
    """Transforma un DataFrame leído desde Bronze para Silver."""
    df = df.rename({column: column.lower() for column in df.columns})
    df = df.with_columns(
        pl.col("tpep_pickup_datetime", "tpep_dropoff_datetime").cast(pl.Datetime, strict=False)
    )
    df = filter_invalid_rows(df)
    df = add_derived_columns(df)
    return standardize_categoricals(df)
