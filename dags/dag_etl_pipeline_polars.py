"""
DAG: dag_etl_pipeline_polars.py
Pipeline modular Bronze -> Silver -> Gold de NYC Taxi con Polars.
Tres tareas independientes para poder reintentar cada capa por separado.
"""

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
if DAG_FOLDER not in sys.path:
    sys.path.insert(0, DAG_FOLDER)

from etl_pipeline_polars.main import run_pipeline
from etl_pipeline_polars.utils import load_config


def extract_bronze(**context):
    cfg = load_config(context["config_path"])
    run_pipeline(cfg, stages=["extract"])


def process_silver(**context):
    cfg = load_config(context["config_path"])
    run_pipeline(cfg, stages=["silver"])


def process_gold(**context):
    cfg = load_config(context["config_path"])
    run_pipeline(cfg, stages=["gold"])


default_args = {
    "owner": "data_team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etl_pipeline_polars",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["nivel2", "nyc_taxi", "polars"],
    description="Pipeline Polars NYC Taxi Bronze -> Silver -> Gold (3 tareas)",
) as dag:

    config_path = os.path.join(DAG_FOLDER, "etl_pipeline_polars", "config.yml")

    task_bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_bronze,
        op_kwargs={"config_path": config_path},
    )

    task_silver = PythonOperator(
        task_id="process_silver",
        python_callable=process_silver,
        op_kwargs={"config_path": config_path},
    )

    task_gold = PythonOperator(
        task_id="process_gold",
        python_callable=process_gold,
        op_kwargs={"config_path": config_path},
    )

    task_bronze >> task_silver >> task_gold
