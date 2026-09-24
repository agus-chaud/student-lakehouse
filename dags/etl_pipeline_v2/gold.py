"""Modelos analiticos Gold y carga en MinIO/PostgreSQL."""

import io
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from etl_pipeline_v2.extract import ensure_bucket
from etl_pipeline_v2.load import load_dataframe_to_postgres

logger = logging.getLogger("etl_pipeline")


GOLD_INPUT_COLUMNS = {
    "tpep_pickup_datetime", "pickup_datetime", "pulocationid", "pulocation_id",
    "dolocationid", "dolocation_id", "payment_type", "fare_amount",
    "tip_amount", "total_amount", "tip_percentage",
}


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _aggregate(data: pd.DataFrame, keys, aggs: dict) -> pd.DataFrame:
    """Agrupa por `keys`; devuelve las claves, trip_count y las agregaciones pedidas."""
    grouped = data.groupby(keys, observed=True)
    result = grouped.size().rename("trip_count").to_frame()
    if aggs:
        result = result.join(grouped.agg(**aggs))
    return result.reset_index()


def transform_gold(df: pd.DataFrame, lookup: pd.DataFrame | None = None) -> dict[str, pd.DataFrame]:
    needed = GOLD_INPUT_COLUMNS
    keep = [column for column in df.columns if str(column).lower() in needed]
    data = df[keep].copy()
    data.columns = [str(column).lower() for column in data.columns]
    data = data.rename(
        columns={
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "pulocationid": "pulocation_id",
            "dolocationid": "dolocation_id",
        }
    )
    if "pickup_datetime" in data:
        data["pickup_datetime"] = pd.to_datetime(data["pickup_datetime"], errors="coerce")

    if lookup is not None and not lookup.empty:
        zones = lookup.copy()
        zones.columns = [str(column).lower() for column in zones.columns]
        zones = zones.rename(columns={"locationid": "location_id"})
        if {"location_id", "zone"}.issubset(zones.columns):
            zones = zones[["location_id", "zone"]].drop_duplicates("location_id")
            if "pulocation_id" in data:
                data = data.merge(
                    zones.rename(columns={"location_id": "pulocation_id", "zone": "pickup_zone"}),
                    on="pulocation_id",
                    how="left",
                )
            if "dolocation_id" in data:
                data = data.merge(
                    zones.rename(columns={"location_id": "dolocation_id", "zone": "dropoff_zone"}),
                    on="dolocation_id",
                    how="left",
                )

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

    # Las agregaciones agrupan directamente sobre `data` (sin copias del DataFrame
    # completo). groupby descarta claves nulas, igual que el dropna previo.
    if "pickup_datetime" in data:
        hour = data["pickup_datetime"].dt.floor("h").rename("pickup_hour")
        models["hourly_demand"] = _aggregate(data, hour, {})
        day = data["pickup_datetime"].dt.floor("D").rename("trip_date")
        daily = _aggregate(
            data, day,
            {c: (c, "sum") for c in ("fare_amount", "tip_amount", "total_amount") if c in data},
        )
        daily["trip_date"] = daily["trip_date"].dt.date
        models["daily_summary"] = daily

    if "pickup_zone" in data:
        zone = data["pickup_zone"].fillna("Unknown")
        aggs = {"total_amount": ("total_amount", "sum")} if "total_amount" in data else {}
        models["zone_performance"] = _aggregate(data, zone, aggs)

    if "payment_type" in data:
        aggs = {}
        if "tip_amount" in data:
            aggs = {"average_tip": ("tip_amount", "mean"), "total_tip": ("tip_amount", "sum")}
        models["tip_analysis"] = _aggregate(data, "payment_type", aggs)
        if "total_amount" in data:
            revenue = _aggregate(data, "payment_type", {"revenue": ("total_amount", "sum")})
        else:
            revenue = _aggregate(data, "payment_type", {})
            revenue["revenue"] = 0.0
        models["revenue_by_payment"] = revenue

    if "pickup_zone" in data and "dropoff_zone" in data:
        aggs = {}
        if "fare_amount" in data:
            aggs["avg_fare_amount"] = ("fare_amount", "mean")
        if "tip_amount" in data:
            aggs["total_tip_amount"] = ("tip_amount", "sum")
        if "tip_percentage" in data:
            aggs["avg_tip_percentage"] = ("tip_percentage", "mean")
        models["route_analysis"] = _aggregate(data, ["pickup_zone", "dropoff_zone"], aggs)

    return models


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buffer)
    return buffer.getvalue()


def persist_gold(
    s3,
    cfg,
    models: dict[str, pd.DataFrame],
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
        metrics[name] = {"rows": len(frame)}
        logger.info("Gold persistido | modelo=%s | filas=%s", name, len(frame))
    return metrics


def run(
    s3,
    cfg: dict,
    silver_df: pd.DataFrame,
    lookup: pd.DataFrame,
    load_postgres: bool = True,
) -> dict[str, pd.DataFrame]:
    """Compatibilidad: transforma y persiste Gold en una sola llamada."""
    models = transform_gold(silver_df, lookup)
    persist_gold(s3, cfg, models, load_postgres=load_postgres)
    return models
