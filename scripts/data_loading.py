from pyspark.sql import SparkSession


class Loading:
    def load_data(self, data: dict, table_name: str):
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("PostgreSQL Connection") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()

        # Create DataFrame from the provided data
        df = spark.createDataFrame([data])

        # Define PostgreSQL connection properties
        postgres_url = "jdbc:postgresql://localhost:5432/postgres_db" #! CHANGE THIS TO READ ENV VARIABLES INSTEAD
        properties = {
            "user": "postgres", #! CHANGE THIS TO READ ENV VARIABLES INSTEAD
            "password": "admin",
            "driver": "org.postgresql.Driver"
        }

        # Write DataFrame to PostgreSQL
        df.write.jdbc(url=postgres_url, table=table_name, mode="append",
                      properties=properties)

        # Stop SparkSession
        spark.stop()
