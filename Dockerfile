# Usa la imagen base oficial de Apache Airflow
FROM apache/airflow:2.10.3

# Cambia al usuario root para instalar dependencias
USER airflow

# Instala el paquete apache-airflow-providers-snowflake
RUN pip install apache-airflow-providers-snowflake apache-airflow-providers-amazon apache-airflow-providers-common-sql
