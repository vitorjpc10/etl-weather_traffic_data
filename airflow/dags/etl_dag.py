import logging
import os
import json
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_extraction import Extract
from scripts.data_transformation import Transform
from scripts.data_loading import Loading

# Add the scripts directory to the sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_path = os.path.join(base_path, 'data', 'extracted')

    # Initialize Extractor
    extract = Extract()

    # Extract weather data
    logging.info("Extracting weather data...")
    weather_data = extract.get_weather(longitude=-74.0060, latitude=40.7128, exclude='hourly,daily', units='metric', lang='en')
    logging.info("Weather data extracted successfully.")

    # Extract traffic data
    logging.info("Extracting traffic data...")
    traffic_data = extract.get_traffic(start_coords=(-74.0060, 40.7128), end_coords=(-122.4194, 37.7749))
    logging.info("Traffic data extracted successfully.")

    write_dict_to_file(weather_data, os.path.join(extracted_path, 'extracted_weather_data.json'))
    write_dict_to_file(traffic_data, os.path.join(extracted_path, 'extracted_traffic_data.json'))

def transform_data():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_path = os.path.join(base_path, 'data', 'extracted')
    transformed_path = os.path.join(base_path, 'data', 'transformed')

    # Load extracted data
    with open(os.path.join(extracted_path, 'extracted_weather_data.json'), 'r') as file:
        weather_data = json.load(file)

    with open(os.path.join(extracted_path, 'extracted_traffic_data.json'), 'r') as file:
        traffic_data = json.load(file)

    # Initialize Transformer
    transform = Transform()

    # Transform weather data
    logging.info("Transforming weather data...")
    weather_data_formatted = transform.clean_weather_data(weather_data)
    logging.info("Weather data transformed successfully.")

    # Transform traffic data
    logging.info("Transforming traffic data...")
    traffic_data_formatted = transform.clean_traffic_data(traffic_data)
    logging.info("Traffic data transformed successfully.")

    write_dict_to_file(weather_data_formatted, os.path.join(transformed_path, 'transformed_weather_data.json'))
    write_dict_to_file(traffic_data_formatted, os.path.join(transformed_path, 'transformed_traffic_data.json'))

def load_data():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    transformed_path = os.path.join(base_path, 'data', 'transformed')

    # Load transformed data
    with open(os.path.join(transformed_path, 'transformed_weather_data.json'), 'r') as file:
        weather_data_formatted = json.load(file)

    with open(os.path.join(transformed_path, 'transformed_traffic_data.json'), 'r') as file:
        traffic_data_formatted = json.load(file)

    # Initialize Loader
    load = Loading()

    load.create_table_if_not_exists("traffic")
    load.create_table_if_not_exists("weather")

    # Load weather data
    logging.info("Loading weather data...")
    load.load_data(weather_data_formatted, "weather")
    logging.info("Weather data loaded successfully.")

    # Load traffic data
    logging.info("Loading traffic data...")
    load.load_data(traffic_data_formatted, "traffic")
    logging.info("Traffic data loaded successfully.")

    load.close_spark()

    logging.info("ETL PROCESSED SUCCESSFULLY")

def write_dict_to_file(data_dict, file_path):
    """
    Write a dictionary to a file in JSON format.

    :param data_dict: Dictionary to write to file
    :param file_path: Path to the file where the dictionary should be written
    """
    try:
        with open(file_path, 'w') as file:
            json.dump(data_dict, file, indent=4)
        logging.info(f"Dictionary written to {file_path} successfully.")
    except Exception as e:
        raise Exception(f"Failed to write dictionary to {file_path}: {e}")

# Define DAG
default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        default_args=default_args,
        dag_id='data_etl_dag',
        description='A DAG for extracting, transforming, and loading data',
        start_date=datetime(2024, 6, 7),
        schedule_interval='@daily'
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task
