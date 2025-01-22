from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime



S3_CONN_ID = "aws_default"
BUCKET_NAME = "gu-dev-raptor"
SQL_FILE_KEY = "supports_main_query_dev.sql"

def read_sql_file_from_s3(bucket_name, key, s3_conn_id):
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    sql_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
    return sql_content

with DAG(
        'push_snowflake_to_dynamo',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@daily',
        catchup=False,
) as dag:

    clean_raptor_model = SnowflakeOperator(
        task_id='clean_raptor_model',
        sql='''
            DROP TABLE DB_GU_DWH.CORE_TABLES.RAPTOR_CACHE;
        ''',
        snowflake_conn_id='snowflake_default',
    )

    update_raptor_model = SnowflakeOperator(
        task_id='update_raptor_model',
        sql=read_sql_file_from_s3(BUCKET_NAME,SQL_FILE_KEY,S3_CONN_ID),
        snowflake_conn_id='snowflake_default',
    )


    run_raptor_snowflake_connector = EcsRunTaskOperator(
        task_id='run_raptor_snowflake_connector',
        cluster='goodunited-default-cluster',
        task_definition='goodunited-raptor-snowflake-connector:3',
        launch_type='FARGATE',
        aws_conn_id='aws_default',
        overrides={
            'containerOverrides': [
                {
                    'name': 'goodunited-raptor-snowflake-connector',
                    'command': ["s3://gu-dev-raptor", "run_job.py"],
                }
            ]
        },
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': ['subnet-0d27cc557db940d2b'],
                'securityGroups': ['sg-097f48adf0d1a1c30'],
            }
        },
    )

    clean_raptor_model >> update_raptor_model >> run_raptor_snowflake_connector
