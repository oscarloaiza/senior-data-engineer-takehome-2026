from datetime import datetime, timedelta
import time
import requests
import json
import psycopg2

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        'stryker_weather_fetcher_v1',
        default_args=default_args,
        description='To fetch the weather data',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['take-home'],
) as dag:

    def fetch_weather_data():
        api_key = "3787bdd14c76e7bd562ba96ab375bf2b"
        cities = ["San Jose,CR", "Heredia,CR", "Cartago,CR", "Alajuela,CR", "Limon,CR"]
        
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
        cursor = conn.cursor()
        
        for city in cities:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                cursor.execute("INSERT INTO staging_weather (city, raw_data) VALUES (%s, %s)",
                            (city, json.dumps(response.json())))
        conn.commit()
        cursor.close()
        conn.close()

    t1 = PythonOperator(
        task_id='ingest_api_data',
        python_callable=fetch_weather_data
    )

    t2 = PostgresOperator(
        task_id="create_raw_dataset",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS staging_weather (
                city VARCHAR(100), 
                raw_data JSONB
            );
            CREATE TABLE IF NOT EXISTS raw_current_weather (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100),
                raw_data JSONB,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           );
          """
    )

    t3 = PostgresOperator(
        task_id="store_dataset",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO raw_current_weather (city, raw_data)
            SELECT city, raw_data FROM staging_weather;
          """
    )

    t2 >> t1 >> t3