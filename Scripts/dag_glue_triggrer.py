from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

default_args = {
    'owner': 'aahash',
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

with DAG(
    dag_id='trigger_glue_merge_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Trigger Glue job for Iceberg table updates'
) as dag:

    trigger_glue_job = GlueJobOperator(
        task_id='run_glue_merge',
        job_name='iceberg_merge_job-Aahash',
        script_args={
            "--JOB_NAME": "iceberg_merge_job-Aahash"
        },
        region_name='ap-south-1',
        wait_for_completion=True
    )

