# Instructions 

## Introduction

Here you have different folders (apis, kafka) and two dockers, with two yaml files:
- **docker-compose.airflow.yaml**: when you run it, it initializes airflow with the different DAGS. 
- **./kafka/docker-compose.kafka.yaml**: when you run it, it simulates an API that is constantly sending data as it comes. 

In **./apis** you can find the code that extracts data from different APIs (or fake APIs).

In **./kafka/kafka_app** you can find the data that comes from a hot path and has to be processed via Kafka. First, you have **/create_kafka_topics** which creates the two main topics: user_registration and user_search. Then, you have **/users_registration** and **/users_search**, where there are two different scripts: one for the kafka producer and the other for the kafka consumer.

Before starting, **REMEMBER** to write down the AWS S3 credentials where it is needed (as we cannot publish the keys in github, these keys are in the project document):
- ./kafka/docker-compose.kafka.yaml : here, there are 3 places where you have to add these keys: in registration-consumer, search-producer and search-consumer.
- ./apis/.env

## How to run the data pipeline?

1. Open **Docker Desktop**.
   
3. Run the following to initialize kafka:
   ./kafka/start_kafka_stack.sh
  
4. Run the following to initialize airflow:
   ./airflow.sh

5. Wait until airflow is inizialized and write into your browser:
   http://localhost:8080/

6. Activate all dags.

**REMEMBER** to close kafka when you finnish in order to not exceed the limit of AWS S3 :(
