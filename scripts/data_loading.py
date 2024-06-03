from pyspark.shell import sql
from sqlalchemy_utils.types.pg_composite import psycopg2

from scripts.data_transformation import weather_df, traffic_df


def insert_weather_data(conn, df):
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            sql.SQL("INSERT INTO weather (dt, temperature, humidity, description) VALUES (%s, %s, %s, %s)"),
            (row['dt'], row['temperature'], row['humidity'], row['description'])
        )
    conn.commit()
    cur.close()

def insert_traffic_data(conn, df):
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            sql.SQL("INSERT INTO traffic (start_time, distance, duration) VALUES (%s, %s, %s)"),
            (row['start_time'], row['distance'], row['duration'])
        )
    conn.commit()
    cur.close()

conn = psycopg2.connect(dbname="your_db", user="your_user", password="your_password", host="your_host")
insert_weather_data(conn, weather_df)
insert_traffic_data(conn, traffic_df)
conn.close()
