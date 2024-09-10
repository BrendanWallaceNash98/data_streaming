import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('sparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.41,"
                    "org.apache.spark:spark-sql-kafka-0.-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'broker') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("spark connection successful")

        return s_conn
    except Exception as e:
        logging.error(f"could not create spark connection {e}")
        return none

def create_casandra_connection():
    try:
        cluster = Cluster(['localhost'])
        return cluster.connect()
    except Exception as e:
        logger.error(f"could not connect to cassandra {e}")
        return None


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        with replication = {'class': 'SimpleStategy', 'replication_factor'
    """)

    print("key space created")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")

def insert_data(session, **kwargs):
    print("inserting data")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data: {e}')

def connec_to_kafka(spark_conn):
    try:




if __name__ == "__manin__":
    spark_conn = create_spark_connection()

    if spark_conn:
        cassandra_conn = create_casandra_connection()

        if cassandra_conn is not None:







