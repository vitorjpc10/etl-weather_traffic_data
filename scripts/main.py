import logging
from data_extraction import Extract
from data_transformation import Transform
from data_loading import Loading

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

    # Print extracted data
    logging.info(f"Weather data: {weather_data}")
    logging.info(f"Traffic data: {traffic_data}")

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

    # Print transformed data
    logging.info(f"Formatted weather data: {weather_data_formatted}")
    logging.info(f"Formatted traffic data: {traffic_data_formatted}")

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

if __name__ == "__main__":
    main()
