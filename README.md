1. Set enviroment variables for api keys in docker-compose (DECIDE ENV_NAME for extract script class) https://home.openweathermap.org/api_keys

2. docker-compose up
3. once build run the following to run queries on both weather and traffic tables
   docker exec -it etl-weather_traffic_data-db-1 psql -U postgres -c "\i queries/queries.sql"

do test python file
write data to data raw for samples
create dag and test airflow

bonus: implement tool for visualization and dockerize

