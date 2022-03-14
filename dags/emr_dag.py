"""
This is an example dag for a AWS EMR Pipeline with auto steps.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

# [START howto_operator_emr_automatic_steps_config]
SPARK_STEPS = [
    {
        'Name': 'process_movie_reviews',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['s3://wz-de-academy-mau-scripts/movie_reviews_job.py', 'MovieReviewsJob', '10'],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'MovieReviews',
    'ReleaseLabel': 'emr-5.29.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'Steps': SPARK_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole'}
# [END howto_operator_emr_automatic_steps_config]

with DAG(
    dag_id='emr_job_flow_automatic_steps_dag',
    dagrun_timeout=timedelta(hours=2),
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['example'],
) as dag:

    # [START howto_operator_emr_automatic_steps_tasks]
    job_flow_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    job_sensor = EmrJobFlowSensor(task_id='check_job_flow', job_flow_id=job_flow_creator.output)
    # [END howto_operator_emr_automatic_steps_tasks]

    # Task dependency created via `XComArgs`:
    #   job_flow_creator >> job_sensor