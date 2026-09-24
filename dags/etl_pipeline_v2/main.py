import os
import sys
from time import perf_counter

import pyarrow.parquet as pq

from etl_pipeline_v2.utils import load_config, measure, setup_logging
from etl_pipeline_v2 import extract
from etl_pipeline_v2.silver import load_silver, transform_silver
from etl_pipeline_v2 import transform_gold
from etl_pipeline_v2 import load_gold

DEFAULT_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yml")


logger = setup_logging()

def row_groups_pipeline(pqfile: pq.ParquetFile):
    """Expone los row groups de Bronze para consumidores compatibles."""
    total = pqfile.num_row_groups
    for index in range(total):
        yield index, total, pqfile.read_row_group(index).to_pandas()


def stage_extract(cfg: dict, s3) -> None:
    """Bronze: descarga viajes y lookup a S3 y valida. No devuelve datos en memoria."""
    logger.info("── ETAPA 1: EXTRACT BRONZE — Viajes ──")
    pqfile = extract.run(cfg, s3=s3)

    logger.info("── ETAPA 2: EXTRACT BRONZE — Lookup ──")
    extract.run_lookup(cfg, s3=s3)
    lookup = extract.read_bronze_csv(s3, cfg["lookup"]["bucket"], cfg["lookup"]["object_path"])

    logger.info("── ETAPA 3: BRONZE — Validación ──")
    extract.validate_bronze(s3, cfg, pqfile, lookup)


def stage_silver(cfg: dict, s3) -> dict:
    """Silver: relee Bronze desde S3, transforma y persiste Silver."""
    logger.info("── ETAPA 4: TRANSFORM + LOAD SILVER ──")
    pqfile = extract.read_bronze_parquet(s3, cfg["bronze"]["bucket"], cfg["bronze"]["object_path"])
    silver_frames = (
        transform_silver.run(pqfile.read_row_group(index).to_pandas())
        for index in range(pqfile.num_row_groups)
    )
    return load_silver.run(cfg, s3, silver_frames)


def stage_gold(cfg: dict, s3) -> dict:
    """Gold: lee Silver y el lookup desde S3, agrega y carga a MinIO/PostgreSQL."""
    logger.info("── ETAPA 5: TRANSFORM + LOAD GOLD ──")
    df_zones = extract.read_bronze_csv(s3, cfg["lookup"]["bucket"], cfg["lookup"]["object_path"])
    # Gold lee de Silver solo las columnas que necesita (evita cargar todo Silver).
    df_silver = load_silver.read_silver(cfg, s3, columns=sorted(transform_gold.GOLD_INPUT_COLUMNS))
    with measure("transform_gold", rows=len(df_silver)):
        gold_models = transform_gold.run(df_silver, df_zones)
    del df_silver
    with measure("load_gold", rows=sum(len(m) for m in gold_models.values())):
        return load_gold.run(cfg, s3, gold_models)


STAGES = {"extract": stage_extract, "silver": stage_silver, "gold": stage_gold}


def run_pipeline(config, stages=None) -> None:
    """Ejecuta las etapas indicadas (por defecto todas, en orden).

    `config` puede ser una ruta a config.yml o un dict ya cargado.
    Las etapas se comunican solo vía S3, por eso pueden correr por separado.
    Ante un error la excepción se propaga (Airflow marca la tarea como fallida).
    """
    stages = list(stages) if stages else list(STAGES)
    unknown = [s for s in stages if s not in STAGES]
    if unknown:
        raise ValueError(f"Etapas desconocidas: {unknown}. Válidas: {list(STAGES)}")

    t_global = perf_counter()
    logger.info("Pipeline ETL iniciado — etapas: %s", stages)

    cfg = load_config(config) if isinstance(config, (str, os.PathLike)) else config
    s3 = extract.build_s3_client(cfg)

    results = {}
    for name in stages:
        try:
            results[name] = STAGES[name](cfg, s3)
        except Exception as error:
            logger.error(f"Fallo en etapa '{name}': {error}", exc_info=True)
            raise

    logger.info("Pipeline finalizado | etapas=%s | tiempo_total=%.2fs", stages, perf_counter() - t_global)
    if results.get("silver"):
        logger.info("  silver_rows=%s", results["silver"]["total_rows"])
    for model_name, metrics in (results.get("gold") or {}).items():
        logger.info("  %s: %s filas → PostgreSQL", model_name, metrics["rows"])


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL Pipeline — NYC Taxi Lakehouse")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--stages", nargs="+", choices=list(STAGES), default=None,
                        help="Etapas a ejecutar (por defecto: todas)")
    args = parser.parse_args()
    try:
        run_pipeline(args.config, stages=args.stages)
    except Exception:
        sys.exit(1)
