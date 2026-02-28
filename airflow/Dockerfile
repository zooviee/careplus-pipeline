FROM apache/airflow:2.9.1

RUN pip install --no-cache-dir \
    boto3 \
    snowflake-connector-python \
    mysql-connector-python \
    dbt-snowflake==1.11.2 \
    python-dotenv
