from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from datetime import timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = "aws_default"
BUCKET_NAME = "gu-dev-airflow-config-files"
SQL_FILE_KEY_1 = "ao-55-script.sql"
SQL_FILE_KEY_2 = "ao-57-script.sql"

def read_sql_file_from_s3(bucket_name, key, s3_conn_id):
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    sql_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
    return sql_content

with DAG(
        'fix_support_data',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@daily',
        catchup=False,
) as dag:


    set_autocommit = SnowflakeOperator(
        task_id="set_autocommit_false",
        sql="ALTER SESSION SET AUTOCOMMIT = FALSE;",
        snowflake_conn_id="snowflake_default"
    )

    update_supporters_set_name = SnowflakeOperator(
        task_id='update_supporters_set_name',
        sql=read_sql_file_from_s3(BUCKET_NAME,SQL_FILE_KEY_1,S3_CONN_ID),
        snowflake_conn_id='snowflake_default',
        execution_timeout=timedelta(minutes=60)
    )

    update_supporters_set_default_name_soft_deleted = SnowflakeOperator(
        task_id='update_supporters_set_default_name_soft_deleted',
        sql=read_sql_file_from_s3(BUCKET_NAME,SQL_FILE_KEY_2,S3_CONN_ID),
        snowflake_conn_id='snowflake_default',
        execution_timeout=timedelta(minutes=60)
    )

    set_autocommit >> update_supporters_set_name >> update_supporters_set_default_name_soft_deleted

