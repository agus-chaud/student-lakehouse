"""
DAG: dag_etl_pipeline_polars.py
Pipeline modular Bronze -> Silver -> Gold de NYC Taxi con Polars.
Tres tareas independientes para poder reintentar cada capa por separado.

Mensual: cada ejecución procesa el mes de su intervalo (data_interval_start).
Con catchup=True, Airflow crea una ejecución por cada mes entre start_date y
end_date (backfill de 2024), de a una por vez.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
if DAG_FOLDER not in sys.path:
    sys.path.insert(0, DAG_FOLDER)

from etl_pipeline_polars.main import run_pipeline
from etl_pipeline_polars.utils import load_config


def _run_stage(stage, config_path, data_interval_start, **_):
    """Corre una etapa para el año y mes del intervalo de esta ejecución."""
    cfg = load_config(config_path)
    run_pipeline(
        cfg,
        stages=[stage],
        year=data_interval_start.year,
        month=data_interval_start.month,
    )


default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_pipeline_polars",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # El último intervalo empieza el 2024-12-01: procesa diciembre de 2024.
    end_date=datetime(2024, 12, 31),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["nivel2", "nyc_taxi", "polars"],
    description="Pipeline Polars NYC Taxi Bronze -> Silver -> Gold, un mes por ejecución",
) as dag:

    config_path = os.path.join(DAG_FOLDER, "etl_pipeline_polars", "config.yml")

    task_bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=_run_stage,
        op_kwargs={"stage": "extract", "config_path": config_path},
    )

    task_silver = PythonOperator(
        task_id="process_silver",
        python_callable=_run_stage,
        op_kwargs={"stage": "silver", "config_path": config_path},
    )

    task_gold = PythonOperator(
        task_id="process_gold",
        python_callable=_run_stage,
        op_kwargs={"stage": "gold", "config_path": config_path},
    )

    task_bronze >> task_silver >> task_gold
