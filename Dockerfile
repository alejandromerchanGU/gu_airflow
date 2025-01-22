# Usa la imagen base oficial de Apache Airflow
FROM apache/airflow:2.5.0

# Cambia al usuario root para instalar dependencias
USER airflow

# Instala el paquete apache-airflow-providers-snowflake
RUN pip install apache-airflow-providers-snowflake apache-airflow-providers-amazon
