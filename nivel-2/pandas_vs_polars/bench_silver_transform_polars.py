import argparse
import time

import polars as pl
import pyarrow.parquet as pq


PAYMENT_TYPE_MAP = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"}
RATECODE_MAP = {
    1: "standard",
    2: "jfk",
    3: "newark",
    4: "nassau",
    5: "negotiated",
    6: "group_ride",
}


def transform_polars(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({c: c.lower() for c in df.columns})

    # Parse datetime si fuera necesario.
    dt_cols = [c for c in df.columns if "datetime" in c.lower()]
    if dt_cols:
        df = df.with_columns([pl.col(c).cast(pl.Datetime, strict=False) for c in dt_cols])

    # Filtros de calidad
    df = df.filter(
        (pl.col("passenger_count").is_not_null() & (pl.col("passenger_count") > 0))
        & (pl.col("trip_distance").is_not_null() & (pl.col("trip_distance") > 0))
        & (pl.col("fare_amount").is_not_null() & (pl.col("fare_amount") >= 0))
        & pl.col("tpep_pickup_datetime").is_not_null()
        & pl.col("tpep_dropoff_datetime").is_not_null()
        & (pl.col("tpep_dropoff_datetime") > pl.col("tpep_pickup_datetime"))
    )

    # Columnas derivadas
    duration_min = (
        (pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).dt.total_seconds() / 60.0
    )

    df = df.with_columns(
        [
            duration_min.round(2).alias("trip_duration_minutes"),
            pl.when(duration_min > 0)
            .then(pl.col("trip_distance") / (duration_min / 60.0))
            .otherwise(None)
            .alias("speed_mph"),
            pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
            # Polars weekday: Monday=1..Sunday=7; ajustar a pandas: Monday=0..Sunday=6
            (pl.col("tpep_pickup_datetime").dt.weekday() - 1).alias("pickup_day_of_week"),
            ((pl.col("tpep_pickup_datetime").dt.weekday() - 1).is_in([5, 6])).alias("is_weekend"),
            pl.when(pl.col("fare_amount") > 0)
            .then((pl.col("tip_amount") / pl.col("fare_amount")) * 100)
            .otherwise(None)
            .round(2)
            .alias("tip_percentage"),
        ]
    )

    # Categóricos (constantes deben ir con pl.lit)
    df = df.with_columns(
        [
            pl.when(pl.col("payment_type") == 1)
            .then(pl.lit(PAYMENT_TYPE_MAP[1]))
            .when(pl.col("payment_type") == 2)
            .then(pl.lit(PAYMENT_TYPE_MAP[2]))
            .when(pl.col("payment_type") == 3)
            .then(pl.lit(PAYMENT_TYPE_MAP[3]))
            .when(pl.col("payment_type") == 4)
            .then(pl.lit(PAYMENT_TYPE_MAP[4]))
            .otherwise(None)
            .alias("payment_type"),
            pl.when(pl.col("ratecodeid") == 1)
            .then(pl.lit(RATECODE_MAP[1]))
            .when(pl.col("ratecodeid") == 2)
            .then(pl.lit(RATECODE_MAP[2]))
            .when(pl.col("ratecodeid") == 3)
            .then(pl.lit(RATECODE_MAP[3]))
            .when(pl.col("ratecodeid") == 4)
            .then(pl.lit(RATECODE_MAP[4]))
            .when(pl.col("ratecodeid") == 5)
            .then(pl.lit(RATECODE_MAP[5]))
            .when(pl.col("ratecodeid") == 6)
            .then(pl.lit(RATECODE_MAP[6]))
            .otherwise(None)
            .alias("ratecodeid"),
            pl.when(pl.col("store_and_fwd_flag") == "Y")
            .then(pl.lit(True))
            .when(pl.col("store_and_fwd_flag") == "N")
            .then(pl.lit(False))
            .otherwise(None)
            .alias("store_and_fwd_flag"),
        ]
    )

    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark Silver transform-only: polars vs Parquet row groups")
    ap.add_argument("--parquet-file", required=True)
    ap.add_argument("--row-groups", type=int, default=None, help="N row groups from start; None=all")
    args = ap.parse_args()

    pqfile = pq.ParquetFile(args.parquet_file)
    total = pqfile.num_row_groups
    n = args.row_groups if args.row_groups is not None else total

    print(f"row_groups_total={total}, benchmarking_n={n}")

    t_total = 0.0
    rows_out_last = 0

    for i in range(n):
        t0 = time.perf_counter()
        table = pqfile.read_row_group(i)
        df_raw = pl.from_arrow(table)
        df_out = transform_polars(df_raw)
        dt = time.perf_counter() - t0

        t_total += dt
        rows_out_last = df_out.height

        print(f"[polars] row_group={i} rows_out={rows_out_last:,} time={dt:.3f}s")

    print(f"[polars] avg_time_per_row_group={(t_total / n):.3f}s")


if __name__ == "__main__":
    main()
