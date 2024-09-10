import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    pass


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


