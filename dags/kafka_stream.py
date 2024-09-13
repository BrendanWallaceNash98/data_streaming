from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import logging
import traceback

default_args = {
    'owner': 'Brendan Wallace Nash',
    'start_date': datetime(2024, 1, 1)
}


def get_data():
    try:
        res = requests.get("https://randomuser.me/api/")
        res.raise_for_status()  # Raises an HTTPError for bad responses
        data = res.json()
        return data['results'][0]
    except requests.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 300:  # 5 minutes
            break
        try:
            res = get_data()
            res = format_data(res)

            future = producer.send('user_data', res)
            record_metadata = future.get(timeout=10)
            logging.info(f"Sent data to Kafka: Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            time.sleep(1)  # Add a small delay between sends
        except KafkaError as e:
            logging.error(f'Failed to send message to Kafka: {e}')
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            logging.error(traceback.format_exc())  # Log the full stack trace
            continue

    producer.close()

def test_kafka_connection():
    try:
        producer = KafkaProducer(bootstrap_servers=['broker:29092'])
        producer.send('test_topic', b'Test message')
        producer.flush()
        producer.close()
        logging.info("Successfully connected to Kafka")
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        raise


with DAG('create_user',
         default_args=default_args,
         schedule='@daily',
         catchup=False
         ) as dag:
    streaming_task = PythonOperator(
        task_id='stream_from_api',
        python_callable=stream_data
    )
    test_kafka_task = PythonOperator(
        task_id='test_kafka_connection',
        python_callable=test_kafka_connection,
        dag=dag
    )


test_kafka_task >> streaming_task



