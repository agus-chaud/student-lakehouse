"""Limpieza, validación y enriquecimiento de la capa Silver."""

import logging

import pandas as pd


logger = logging.getLogger("etl_pipeline.silver.transform")


def filter_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina viajes con métricas, fechas o duración inválidas."""
    before = len(df)
    result = df[
        df["passenger_count"].notna()
        & (df["passenger_count"] > 0)
        & df["trip_distance"].notna()
        & (df["trip_distance"] > 0)
        & df["fare_amount"].notna()
        & (df["fare_amount"] >= 0)
        & df["tpep_pickup_datetime"].notna()
        & df["tpep_dropoff_datetime"].notna()
        & (df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"])
    ].copy()
    logger.info(
        "Filtro de calidad: %s → %s filas (%s descartadas)",
        f"{before:,}",
        f"{len(result):,}",
        f"{before - len(result):,}",
    )
    return result.reset_index(drop=True)


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula duración, velocidad, atributos temporales y porcentaje de propina."""
    result = df.copy()
    duration = (
        result["tpep_dropoff_datetime"] - result["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    result["trip_duration_minutes"] = duration.round(2)
    result["speed_mph"] = (result["trip_distance"] / (duration / 60)).where(duration > 0)
    result["pickup_hour"] = result["tpep_pickup_datetime"].dt.hour
    result["pickup_day_of_week"] = result["tpep_pickup_datetime"].dt.dayofweek
    result["is_weekend"] = result["pickup_day_of_week"].isin([5, 6])
    result["tip_percentage"] = (
        (result["tip_amount"] / result["fare_amount"]) * 100
    ).where(result["fare_amount"] > 0).round(2)
    logger.debug(
        "Columnas derivadas agregadas: trip_duration_minutes, speed_mph, "
        "pickup_hour, pickup_day_of_week, is_weekend, tip_percentage"
    )
    return result


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


def standardize_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte códigos TLC a etiquetas y flags Y/N a booleanos."""
    result = df.copy()
    result["payment_type"] = result["payment_type"].map(PAYMENT_TYPE_MAP)
    result["ratecodeid"] = result["ratecodeid"].map(RATECODE_MAP)
    result["store_and_fwd_flag"] = result["store_and_fwd_flag"].map(
        {"Y": True, "N": False}
    )
    return result


def run(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma un DataFrame completo leído desde Bronze para Silver."""
    result = df.copy()
    result.columns = [str(column).lower() for column in result.columns]
    result["tpep_pickup_datetime"] = pd.to_datetime(
        result["tpep_pickup_datetime"], errors="coerce"
    )
    result["tpep_dropoff_datetime"] = pd.to_datetime(
        result["tpep_dropoff_datetime"], errors="coerce"
    )
    result = filter_invalid_rows(result)
    result = add_derived_columns(result)
    return standardize_categoricals(result)