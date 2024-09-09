from airflow import DAG
from datetime import datetime,  timedelta
import json
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd



# function to convert from kelvin to fahrenheit
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit



def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    # to get aws credentials >> aws sts get-session-token 
    # after you configure aws

    aws_credentials = {
        "key": "----",
        "secret": "----",
        "token": "----",
    }

    # just to make file name unique by time
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_egypt_' + dt_string
    
    # type your s3 bucket name
    df_data.to_csv(f"s3://weather-api-airflow-p02/{dt_string}.csv", index=False, storage_options=aws_credentials)


# Default arguments for the DAG
default_args = {
    'owner': 'doaa_alshorbagy',
    'start_date': datetime(2024, 8, 11),
    'email': ['d2alsho@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)

}

# Define the DAG
with DAG(
    dag_id='Airflow_api',
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False
) as dag:

    # Note: create airflow http connection with https://openweathermap.org

    # To get APPID you must have an account to weather_api
    is_weather_api_ready = HttpSensor(
    task_id ='is_weather_api_ready',
    http_conn_id='weathermap_api',
    endpoint='/data/2.5/weather?q=Egypt&APPID=9391b1ed3b3a407f0460f89c4b3da190',
    response_error_codes_allowlist=None, # To avoid failing the task for other codes than 404
    )


    extract_weather_data = SimpleHttpOperator(
    task_id = 'extract_weather_data',
    http_conn_id = 'weathermap_api',
    endpoint='/data/2.5/weather?q=Egypt&APPID=9391b1ed3b3a407f0460f89c4b3da190',
    method = 'GET',
    response_filter=lambda response: json.loads(response.text),
    log_response=True
    )


    transform_load_weather_data = PythonOperator(
    task_id= 'transform_load_weather_data',
    python_callable=transform_load_data
    )


is_weather_api_ready >> extract_weather_data >> transform_load_weather_data