import psycopg2
from psycopg2 import sql

def create_tables():
    commands = [
        """
        CREATE TABLE IF NOT EXISTS weather (
            id SERIAL PRIMARY KEY,
            dt TIMESTAMP NOT NULL,
            temperature FLOAT NOT NULL,
            humidity INT NOT NULL,
            description TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS traffic (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            distance TEXT NOT NULL,
            duration TEXT NOT NULL
        )
        """
    ]
    conn = None
    try:
        conn = psycopg2.connect(dbname="your_db", user="your_user", password="your_password", host="your_host")
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

create_tables()
