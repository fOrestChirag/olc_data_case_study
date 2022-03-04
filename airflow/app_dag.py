from airflow.contrib.operators.emr_create_job_flow_operator import (EmrCreateJobFlowOperator)
from airflow.contrib.operators.emr_terminate_job_flow_operator import (EmrTerminateJobFlowOperator)
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta, datetime
import os

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 30, 8, 00),
    "schedule_interval": timedelta(hours=12),
    "email": ['mailforchirag.s@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    "catchup": False,
}

DAG_ID = "adidas-case-study"
BUCKET_NAME = "olc-dump"
script = "python_script/olc_load.py"

JOB_FLOW_OVERRIDES = {
    "Name": "olc-data-load",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name": "Hive"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

SPARK_TASK = [
    {
        'Name': 'olc_data_clean_load',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ["spark-submit", "--deploy-mode", "client", "s3://{{ params.BUCKET_NAME }}/{{ params.python_script }}"],
        },
    }
]

with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        start_date=datetime(2022, 3, 30, 8, 00),
        schedule_interval=timedelta(hours=12),
        catchup=False,
        max_active_runs=1
) as dag:
    # Create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        dag=dag,
    )

    # Add spark steps to EMR cluster
    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_TASK,
        params={
            "BUCKET_NAME": BUCKET_NAME,
            "s3_script": script,
        },
        dag=dag,
    )

    # Wait for the steps to complete
    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        dag=dag,
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        dag=dag,
    )
