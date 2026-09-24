import argparse
import time

import pandas as pd
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


def transform_pandas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # Normalización mínima de tipos: float64 -> Int64 cuando corresponde.
    for col in df.columns:
        if df[col].dtype == "float64":
            s = df[col]
            if ((s % 1 == 0) | s.isnull()).all():
                df[col] = s.astype("Int64")

    # Parse datetime (siempre que exista una columna que parezca datetime)
    for col in df.columns:
        if "datetime" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Filtros de calidad
    before = len(df)
    df = df[
        (df["passenger_count"].notna() & (df["passenger_count"] > 0))
        & (df["trip_distance"].notna() & (df["trip_distance"] > 0))
        & (df["fare_amount"].notna() & (df["fare_amount"] >= 0))
        & (df["tpep_pickup_datetime"].notna())
        & (df["tpep_dropoff_datetime"].notna())
        & (df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"])
    ].reset_index(drop=True)

    # Columnas derivadas
    duration = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60.0
    df["trip_duration_minutes"] = duration.round(2)

    df["speed_mph"] = (df["trip_distance"] / (duration / 60.0)).where(duration > 0)
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek  # Monday=0
    df["is_weekend"] = df["pickup_day_of_week"].isin([5, 6])

    df["tip_percentage"] = ((df["tip_amount"] / df["fare_amount"]) * 100).where(df["fare_amount"] > 0).round(2)

    # Categóricos
    df["payment_type"] = df["payment_type"].map(PAYMENT_TYPE_MAP)
    df["ratecodeid"] = df["ratecodeid"].map(RATECODE_MAP)
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].map({"Y": True, "N": False})

    after = len(df)
    # (Opcional) evita imprimir mucho; el benchmark mide tiempo.
    _ = before, after
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark Silver transform-only: pandas vs Parquet row groups")
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
        df_raw = table.to_pandas()
        df_out = transform_pandas(df_raw)
        dt = time.perf_counter() - t0

        t_total += dt
        rows_out_last = len(df_out)

        print(f"[pandas] row_group={i} rows_out={rows_out_last:,} time={dt:.3f}s")

    print(f"[pandas] avg_time_per_row_group={(t_total / n):.3f}s")


if __name__ == "__main__":
    main()
