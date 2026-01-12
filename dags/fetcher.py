from datetime import datetime, timedelta
import time
import requests
import json
import psycopg2
from airflow import DAG
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
    
    #This function connects to the OpenWeatherMap API and fetches weather data for the cities specified in a list. 
    #It then inserts the data into a table so that it can be used later in T3
    def fetch_weather_data():
        #My API key
        api_key = "3787bdd14c76e7bd562ba96ab375bf2b"
        #The list of cities whose data we want to obtain
        cities = ["San Jose,CR", "Heredia,CR", "Cartago,CR", "Alajuela,CR", "Limon,CR"]
        
        #DB Connection
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
        #Cursor to execute queries in the DB
        cursor = conn.cursor()
        
        #FOR loop to extract data for each city
        for city in cities:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
            #Extract data
            response = requests.get(url)
            if response.status_code == 200:
                #Insert city data into a provisional table
                cursor.execute("INSERT INTO staging_weather (city, raw_data) VALUES (%s, %s)",
                            (city, json.dumps(response.json())))
                
        #Ensure data is saved and transaction is finalized
        conn.commit()
        #Close the cursor and the connection
        cursor.close()
        conn.close()

    t1 = PythonOperator(
        task_id='ingest_api_data',
        python_callable=fetch_weather_data
    )

    #Create the tables needed for the exercise
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

    #Save the data in the raw_current_weather table of the database
    t3 = PostgresOperator(
        task_id="store_dataset",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO raw_current_weather (city, raw_data)
            SELECT city, raw_data FROM staging_weather;
          """
    )
    #Task order
    #T2 is executed first because T1 requires the tables to be created in order to extract and insert the weather data correctly
    t2 >> t1 >> t3