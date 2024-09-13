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
        logging.info("Attempting to create Spark connection")
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("INFO")
        logging.info("Spark connection created successfully")
        return s_conn
    except Exception as e:
        logging.error(f"Could not create spark connection: {str(e)}", exc_info=True)
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(
            ['cassandra_db'],
            port=9042,
            protocol_version=4,
            connect_timeout=30
        )
        session = cluster.connect(wait_for_all_pools=True)
        logging.info("Successfully connected to Cassandra")
        return session
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {str(e)}", exc_info=True)
        return None
def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace created successfully")
    except Exception as e:
        logging.error(f"Failed to create keyspace: {str(e)}", exc_info=True)


def create_table(session):
    try:
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
    except Exception as e:
        logging.error(f"Failed to create table: {str(e)}", exc_info=True)


def insert_data(session, **kwargs):
    try:
        user_id = kwargs.get('id')
        if user_id:
            try:
                user_id = UUID(user_id)
            except ValueError:
                logging.warning(f"Invalid UUID '{user_id}'. Generating a new UUID.")
                user_id = uuid.uuid4()
        else:
            user_id = uuid.uuid4()

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
        logging.info(f"Data inserted for {first_name} {last_name} with ID {user_id}")
    except Exception as e:
        logging.error(f'Could not insert data: {str(e)}', exc_info=True)
        logging.error(f'Attempted to insert: {kwargs}')


def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "user_data") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Successfully connected to Kafka")
        logging.info(f"Kafka schema: {spark_df.printSchema()}")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {str(e)}", exc_info=True)
        raise


def kafka_to_df(spark_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])

    return spark_df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logging.info(f"Batch {batch_id} is empty")
    else:
        logging.info(f"Processing batch {batch_id}")
        logging.info(f"Batch content:")
        batch_df.show(truncate=False)

        rows = batch_df.collect()
        for row in rows:
            data = row.asDict()
            logging.info(f"Inserting data: {data}")
            try:
                insert_data(cassandra_conn, **data)
                logging.info(f"Successfully inserted data for {data.get('first_name')} {data.get('last_name')}")
            except Exception as e:
                logging.error(f"Failed to insert data: {str(e)}", exc_info=True)

    logging.info(f"Finished processing batch {batch_id}")


if __name__ == "__main__":
    spark_conn = None
    cassandra_conn = None
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Starting Spark Streaming application")

        spark_conn = create_spark_connection()
        if not spark_conn:
            raise Exception("Failed to create Spark connection")

        spark_df = connect_to_kafka(spark_conn)
        df = kafka_to_df(spark_df)
        logging.info("Created DataFrame from Kafka stream")

        cassandra_conn = create_cassandra_connection()
        if not cassandra_conn:
            raise Exception("Failed to create Cassandra connection")

        create_keyspace(cassandra_conn)
        create_table(cassandra_conn)

        streaming_query = (df.writeStream
                           .foreachBatch(process_batch)
                           .option('checkpointLocation', '/tmp/checkpoint')
                           .trigger(processingTime='5 seconds')
                           .start())

        logging.info("Streaming query started")
        streaming_query.awaitTermination(timeout=300)
        logging.info("Streaming query completed")

    except Exception as e:
        logging.error("An error occurred in the main execution:")
        logging.error(traceback.format_exc())
    finally:
        if spark_conn:
            logging.info("Stopping Spark session")
            spark_conn.stop()
        if cassandra_conn:
            logging.info("Closing Cassandra connection")
            cassandra_conn.shutdown()
        logging.info("Spark Streaming application stopped")