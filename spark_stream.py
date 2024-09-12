import logging
from uuid import UUID
import uuid
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import traceback


def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .getOrCreate()
        return s_conn
    except Exception as e:
        logging.error(f"could not create spark connection {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra_db'])
        return cluster.connect()
    except Exception as e:
        logging.error(f"could not connect to cassandra {e}")
        return None


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created")


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
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    logging.info("Table created successfully!")


def insert_data(session, **kwargs):
    try:
        user_id = UUID(kwargs.get('id')) if kwargs.get('id') else uuid.uuid4()
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

        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f'could not insert data: {str(e)}', exc_info=True)
        raise


def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "user_data") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Successfully connected to Kafka")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {str(e)}", exc_info=True)
        raise


def kafka_to_df(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel


def process_batch(batch_df, batch_id):
    logging.info(f"Processing batch {batch_id}")
    logging.info(f"Batch content: {batch_df.show(truncate=False)}")

    rows = batch_df.collect()
    for row in rows:
        data = row.asDict()
        insert_data(cassandra_conn, **data)
    logging.info(f"Inserted batch {batch_id} into Cassandra")


if __name__ == "__main__":
    spark_conn = None
    cassandra_conn = None
    try:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Starting Spark Streaming application")

        spark_conn = create_spark_connection()
        logging.info("Spark connection created")

        if spark_conn:
            spark_df = connect_to_kafka(spark_conn)
            logging.info("Connected to Kafka")
            df = kafka_to_df(spark_df)
            logging.info("Created DataFrame from Kafka stream")

            cassandra_conn = create_cassandra_connection()
            logging.info("Cassandra connection created")

            if cassandra_conn is not None:
                create_keyspace(cassandra_conn)
                create_table(cassandra_conn)

                streaming_query = (df.writeStream
                                   .foreachBatch(process_batch)
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .start())

                logging.info("Streaming query started")
                streaming_query.awaitTermination()
            else:
                logging.error("Failed to create Cassandra connection")
        else:
            logging.error("Failed to create Spark connection")

    except Exception as e:
        logging.error("An error occurred in the main execution:")
        logging.error(traceback.format_exc())
    finally:
        # Cleanup
        if spark_conn:
            logging.info("Stopping Spark session")
            spark_conn.stop()
        if cassandra_conn:
            logging.info("Closing Cassandra connection")
            cassandra_conn.shutdown()
        logging.info("Spark Streaming application stopped")