
This project was a chance to work with many in a more cohesive manner. I have worked with Airflow, Cassandra and Spark before but all as individual tools but never in a full pipeline. Kafka it a tool that I have been looking to use for a while so integrating it as the message broker between Airflow and Spark 
was a good opportunity to get hands on experience with it. 

For data generation, I utilized the Random User API, which provides an endpoint for requesting PII data of AI-generated fake individuals. Although its request limits aren't suitable for large-scale systems, it offers a reliable and generous quota for this project's scope.

## System Design
This pipeline uses airflow as the scheduler of a single DAG to retrieve user information from the endpoint. The DAG will continue to make request for 5 minutes before it ends. The DAG has built in data formatting and validation and finally passes the formatted data to the Kafka message broker. SQL Spark is used to collect the data from the message broker. In the Spark worker a UUID is added to the worker before inserting it into the Cassandra database. The Spark workers are managed in a python script, where connections are tested for Kafka and Cassandra.

## Distributed System?
In its current form I only have one Spark Worker and one Kafka Broker meaning the distributed system nature of both tools is not being utilised. This also makes the use of a tool 
like Zookeeper redundant. 
If I was to try and spin this as logical decision and not just a case of running it on my laptop and seeing no need for aiming a one request every 10 second pipeline distributed, I would say that having the random user API in this sense be the equivariant of a new user creating endpoint for a new website the current set up is perfect for the demands but it allows for a lot of horizonal scaling to allow for this system to grow along with the website. 
As the throughput of the created user endpoint becomes large more Kafka brokers and Spark workers can be spun up to handle the larger throughput. In terms of using Cassandra, it is one of the most scalable databases and can be used across many nodes. For much larger Scylla DB is also a good option and as its built-on top of Cassandra it is very easy to migrate your date to it.

## Learning
While I did not take advantage of distributed nature of Kafka it was easy to see why it is liked for its reliance. When testing Spark builds may lag and I'll have already run the DAG and 
data will be rolling through. Once Spark is finally up and running Kafka will send through the data and we will have no data loss. This will only get more resilient when Kafka is used with more brokers. A distributed Kafka system can replicate its data across its ![Uploading image.png…]()
