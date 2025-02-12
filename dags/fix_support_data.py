from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from datetime import datetime
from datetime import timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = "aws_default"
BUCKET_NAME = "gu-dev-airflow-config-files"
SQL_FILE_KEY_1 = "ao-55-script.sql"
SQL_FILE_KEY_2 = "ao-57-script.sql"

_SNOWFLAKE_DB = "DB_GU_DWH"
_SNOWFLAKE_SCHEMA = "CORE_TABLES"
_SNOWFLAKE_TABLE = "SUPPORTER"

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


    update_supporters_set_name = SnowflakeSqlApiOperator(
        task_id='update_supporters_set_name',
        snowflake_conn_id='snowflake_default',
        sql=read_sql_file_from_s3(BUCKET_NAME,SQL_FILE_KEY_1,S3_CONN_ID),
        statement_count=1,
        autocommit=False
    )


    update_supporters_set_default_name_soft_deleted = SnowflakeSqlApiOperator(
        task_id='update_supporters_set_default_name_soft_deleted',
        snowflake_conn_id='snowflake_default',
        sql=read_sql_file_from_s3(BUCKET_NAME,SQL_FILE_KEY_2,S3_CONN_ID),
        statement_count=1,
        autocommit=False
    )

    update_supporters_set_name >> update_supporters_set_default_name_soft_deleted
