"""Modelos analiticos Gold y carga en MinIO/PostgreSQL."""

import io
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from extract import ensure_bucket
from load import load_dataframe_to_postgres

logger = logging.getLogger("etl_pipeline")


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def transform_gold(df: pd.DataFrame, lookup: pd.DataFrame | None = None) -> dict[str, pd.DataFrame]:
    data = df.copy()
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

    if "pickup_datetime" in data:
        hourly = data.dropna(subset=["pickup_datetime"]).copy()
        hourly["pickup_hour"] = hourly["pickup_datetime"].dt.floor("h")
        models["hourly_demand"] = hourly.groupby("pickup_hour", as_index=False).size().rename(columns={"size": "trip_count"})
        daily = hourly.copy()
        daily["trip_date"] = daily["pickup_datetime"].dt.date
        daily_summary = daily.groupby("trip_date", as_index=False).size().rename(columns={"size": "trip_count"})
        for column in ("fare_amount", "tip_amount", "total_amount"):
            if column in daily:
                grouped = daily.groupby("trip_date", as_index=False)[column].sum()
                daily_summary = daily_summary.merge(grouped, on="trip_date")
        models["daily_summary"] = daily_summary

    if "pickup_zone" in data:
        zone = data.copy()
        zone["pickup_zone"] = zone["pickup_zone"].fillna("Unknown")
        grouped = zone.groupby("pickup_zone", as_index=False).size().rename(columns={"size": "trip_count"})
        models["zone_performance"] = grouped
        if "total_amount" in zone:
            totals = zone.groupby("pickup_zone", as_index=False)["total_amount"].sum()
            models["zone_performance"] = grouped.merge(totals, on="pickup_zone")

    if "payment_type" in data:
        tip = data.copy()
        grouped = tip.groupby("payment_type", as_index=False).size().rename(columns={"size": "trip_count"})
        models["tip_analysis"] = grouped
        if "tip_amount" in tip:
            values = tip.groupby("payment_type", as_index=False)["tip_amount"].agg(
                average_tip="mean", total_tip="sum"
            )
            models["tip_analysis"] = grouped.merge(values, on="payment_type")
        revenue = tip.groupby("payment_type", as_index=False).size().rename(columns={"size": "trip_count"})
        if "total_amount" in tip:
            values = tip.groupby("payment_type", as_index=False)["total_amount"].sum().rename(columns={"total_amount": "revenue"})
            revenue = revenue.merge(values, on="payment_type")
        else:
            revenue["revenue"] = 0.0
        models["revenue_by_payment"] = revenue

    if "pickup_zone" in data and "dropoff_zone" in data:
        route = data.dropna(subset=["pickup_zone", "dropoff_zone"]).copy()
        grouped = route.groupby(["pickup_zone", "dropoff_zone"], as_index=False).size().rename(columns={"size": "trip_count"})
        if "fare_amount" in route:
            fare = route.groupby(["pickup_zone", "dropoff_zone"], as_index=False)["fare_amount"].mean().rename(columns={"fare_amount": "avg_fare_amount"})
            grouped = grouped.merge(fare, on=["pickup_zone", "dropoff_zone"])
        if "tip_amount" in route:
            tip_totals = route.groupby(["pickup_zone", "dropoff_zone"], as_index=False)["tip_amount"].sum().rename(columns={"tip_amount": "total_tip_amount"})
            grouped = grouped.merge(tip_totals, on=["pickup_zone", "dropoff_zone"])
        if "tip_percentage" in route:
            tip_pct = route.groupby(["pickup_zone", "dropoff_zone"], as_index=False)["tip_percentage"].mean().rename(columns={"tip_percentage": "avg_tip_percentage"})
            grouped = grouped.merge(tip_pct, on=["pickup_zone", "dropoff_zone"])
        models["route_analysis"] = grouped

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
