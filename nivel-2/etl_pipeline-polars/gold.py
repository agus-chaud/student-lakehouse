"""Modelos analiticos Gold (Polars) y carga en MinIO/PostgreSQL."""

import io
import logging

import polars as pl

from extract import ensure_bucket
from load import load_dataframe_to_postgres

logger = logging.getLogger("etl_pipeline")


GOLD_INPUT_COLUMNS = {
    "tpep_pickup_datetime", "pickup_datetime", "pulocationid", "pulocation_id",
    "dolocationid", "dolocation_id", "payment_type", "fare_amount",
    "tip_amount", "total_amount", "tip_percentage",
}


def _empty(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.String for column in columns})


def _aggregate(data: pl.DataFrame, keys: list[str], aggs: list[pl.Expr]) -> pl.DataFrame:
    """Agrupa por `keys`; devuelve las claves, trip_count y las agregaciones pedidas.

    Descarta claves nulas (igual que groupby de pandas) y ordena por clave
    para que el resultado sea determinista.
    """
    return (
        data.drop_nulls(keys)
        .group_by(keys)
        .agg(pl.len().alias("trip_count"), *aggs)
        .sort(keys)
    )


def transform_gold(df: pl.DataFrame, lookup: pl.DataFrame | None = None) -> dict[str, pl.DataFrame]:
    keep = [column for column in df.columns if column.lower() in GOLD_INPUT_COLUMNS]
    data = df.select(keep).rename({column: column.lower() for column in keep})
    data = data.rename(
        {
            old: new
            for old, new in {
                "tpep_pickup_datetime": "pickup_datetime",
                "pulocationid": "pulocation_id",
                "dolocationid": "dolocation_id",
            }.items()
            if old in data.columns
        }
    )
    if "pickup_datetime" in data.columns:
        data = data.with_columns(pl.col("pickup_datetime").cast(pl.Datetime, strict=False))

    if lookup is not None and not lookup.is_empty():
        zones = lookup.rename({column: column.lower() for column in lookup.columns})
        zones = zones.rename({"locationid": "location_id"}) if "locationid" in zones.columns else zones
        if {"location_id", "zone"}.issubset(zones.columns):
            zones = zones.select("location_id", "zone").unique("location_id")
            if "pulocation_id" in data.columns:
                data = data.join(
                    zones.rename({"location_id": "pulocation_id", "zone": "pickup_zone"}),
                    on="pulocation_id",
                    how="left",
                )
            if "dolocation_id" in data.columns:
                data = data.join(
                    zones.rename({"location_id": "dolocation_id", "zone": "dropoff_zone"}),
                    on="dolocation_id",
                    how="left",
                )

    cols = set(data.columns)
    models = {
        "hourly_demand": _empty(["pickup_hour", "trip_count"]),
        "zone_performance": _empty(["pickup_zone", "trip_count", "total_amount"]),
        "tip_analysis": _empty(["payment_type", "trip_count", "average_tip", "total_tip"]),
        "daily_summary": _empty(["trip_date", "trip_count", "fare_amount", "tip_amount", "total_amount"]),
        "revenue_by_payment": _empty(["payment_type", "trip_count", "revenue"]),
        "route_analysis": _empty([
            "pickup_zone", "dropoff_zone", "trip_count",
            "avg_fare_amount", "total_tip_amount", "avg_tip_percentage",
        ]),
    }

    if "pickup_datetime" in cols:
        pickup = pl.col("pickup_datetime")
        models["hourly_demand"] = _aggregate(
            data.with_columns(pickup.dt.truncate("1h").alias("pickup_hour")), ["pickup_hour"], []
        )
        daily_sums = [pl.col(c).sum() for c in ("fare_amount", "tip_amount", "total_amount") if c in cols]
        models["daily_summary"] = _aggregate(
            data.with_columns(pickup.dt.date().alias("trip_date")), ["trip_date"], daily_sums
        )

    if "pickup_zone" in cols:
        aggs = [pl.col("total_amount").sum()] if "total_amount" in cols else []
        models["zone_performance"] = _aggregate(
            data.with_columns(pl.col("pickup_zone").fill_null("Unknown")), ["pickup_zone"], aggs
        )

    if "payment_type" in cols:
        aggs = []
        if "tip_amount" in cols:
            aggs = [
                pl.col("tip_amount").mean().alias("average_tip"),
                pl.col("tip_amount").sum().alias("total_tip"),
            ]
        models["tip_analysis"] = _aggregate(data, ["payment_type"], aggs)
        if "total_amount" in cols:
            revenue = _aggregate(data, ["payment_type"], [pl.col("total_amount").sum().alias("revenue")])
        else:
            revenue = _aggregate(data, ["payment_type"], []).with_columns(revenue=pl.lit(0.0))
        models["revenue_by_payment"] = revenue

    if {"pickup_zone", "dropoff_zone"} <= cols:
        aggs = []
        if "fare_amount" in cols:
            aggs.append(pl.col("fare_amount").mean().alias("avg_fare_amount"))
        if "tip_amount" in cols:
            aggs.append(pl.col("tip_amount").sum().alias("total_tip_amount"))
        if "tip_percentage" in cols:
            aggs.append(pl.col("tip_percentage").mean().alias("avg_tip_percentage"))
        models["route_analysis"] = _aggregate(data, ["pickup_zone", "dropoff_zone"], aggs)

    return models


def _parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


def persist_gold(
    s3,
    cfg,
    models: dict[str, pl.DataFrame],
    load_postgres: bool = True,
) -> dict[str, dict[str, int]]:
    destination = cfg["gold"]
    ensure_bucket(s3, destination["bucket"])
    metrics = {}
    for name, model_cfg in destination["models"].items():
        frame = models.get(name, _empty([]))
        s3.put_object(
            Bucket=destination["bucket"],
            Key=model_cfg["object_path"],
            Body=_parquet_bytes(frame),
        )
        if load_postgres:
            load_dataframe_to_postgres(cfg, model_cfg["table"], frame)
        metrics[name] = {"rows": frame.height}
        logger.info("Gold persistido | modelo=%s | filas=%s", name, frame.height)
    return metrics
