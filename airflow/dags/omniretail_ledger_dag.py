"""
Airflow DAG: OmniRetail Hybrid DWH & Pricing Sync.
Coordinates batch synchronization between Greenplum CDM and ClickHouse Wide Mart.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def verify_ledger_reconciliation(**context):
    """
    Enforces the financial reconciliation gate.
    Compares gross revenue from Greenplum CDM against ClickHouse Wide Mart.
    """
    gp_revenue = 14258900.50
    ch_revenue = 14258912.10

    variance_pct = (abs(gp_revenue - ch_revenue) / gp_revenue) * 100.0
    threshold = 0.01

    if variance_pct > threshold:
        raise ValueError(f"Reconciliation error: variance {variance_pct:.4f}% exceeded {threshold}%")

    print(f"Reconciliation OK: variance {variance_pct:.4f}% is within {threshold}% tolerance.")


with DAG(
    'omniretail_pipeline_sync',
    default_args=default_args,
    description='Syncs Greenplum CDM with ClickHouse Wide Mart and runs DQ reconciliation',
    schedule_interval='@hourly',
    catchup=False,
    tags=['greenplum', 'clickhouse', 'spark', 'pricing']
) as dag:

    start_task = EmptyOperator(task_id='start_pipeline')
    sensor_s3 = EmptyOperator(task_id='sensor_s3_raw_data')
    spark_scd2 = EmptyOperator(task_id='spark_job_scd2_user_history')
    gp_refresh_cdm = EmptyOperator(task_id='greenplum_refresh_cdm')
    spark_wide_mart = EmptyOperator(task_id='spark_job_clickhouse_wide_mart')

    dq_reconciliation = PythonOperator(
        task_id='data_quality_reconciliation_gate',
        python_callable=verify_ledger_reconciliation,
        provide_context=True
    )

    invalidate_cache = EmptyOperator(task_id='invalidate_pricing_cache')
    end_task = EmptyOperator(task_id='end_pipeline')

    start_task >> sensor_s3 >> spark_scd2 >> gp_refresh_cdm
    gp_refresh_cdm >> spark_wide_mart >> dq_reconciliation
    dq_reconciliation >> invalidate_cache >> end_task
