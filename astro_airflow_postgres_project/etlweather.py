from airflow import DAG
# http hook
from airflow.providers.http.hooks.http import HttpHook
# postgres hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import json

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
HTTP_CONN_ID = "open-meteo"  # Fixed variable name

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1)
}

@dag(
    dag_id="etlweather",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)
def etlweather():

    @task
    def get_weather():
        """Extract weather data from Open-Meteo API using airflow connection"""
        
        http_hook = HttpHook(http_conn_id=HTTP_CONN_ID, method='GET')  # Use correct variable name

        ## Build the API endpoint
        ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        response = http_hook.run(endpoint=endpoint)

        # Check if the request was successful
        if response.status_code == 200:  # Fixed typo in 'condition'
            return response.json()
        else:
            raise Exception(f"API request failed with status code {response.status_code}")

    @task
    def transform_weather(weatherdata):
        """Transform weather data"""
        
        current_weather = weatherdata['current_weather']
        transformed_weather = {
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'time': current_weather['time'],
            'latitude': LATITUDE,
            'longitude': LONGITUDE
        }
        return transformed_weather

    @task
    def load_weather(weatherdata):
        """Load weather data"""

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                id SERIAL PRIMARY KEY,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode FLOAT,
                time timestamp,
                latitude FLOAT,
                longitude FLOAT
            )
        """)
        
        # Insert data
        cursor.execute(
            "INSERT INTO weather (temperature, windspeed, winddirection, weathercode, time, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                weatherdata['temperature'],
                weatherdata['windspeed'],
                weatherdata['winddirection'],
                weatherdata['weathercode'],
                weatherdata['time'],
                weatherdata['latitude'],
                weatherdata['longitude']
            )
        )
        conn.commit()
        cursor.close()
        conn.close()

    @task
    def print_weather(weatherdata):
        """Print weather data"""
        print(weatherdata)

    # Define the task dependencies
    weather = get_weather()
    transformed_weather = transform_weather(weather)
    load_weather(transformed_weather)
    print_weather(transformed_weather) 
    
# Instantiate the DAG
etl_weather_dag = etlweather()

# Run the DAG
etl_weather_dag