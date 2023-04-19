# Confluent Kafka

A simple ETL pipeline integrating MYSQL, Kafka and Cassandra.


## Project Description

1. Random data is generated and loaded into MYSQL table using data_generator.py
2. Data from MYSQL table is sent to Kafka topic using producer.py
3. Data from Kafka topic is consumed and written to cassandra table using consumer.py

## Resources:
1. Confluent Cloud for Kafka
2. DataStax Astra for Cassandra

## Images

<p align="center">
  <img src="https://github.com/Pranjal-Tripathi-01/Kafka/blob/main/Screenshot%20from%202023-04-19%2021-29-17.png" title="MYSQL">
  <img src="https://github.com/Pranjal-Tripathi-01/Kafka/blob/main/Screenshot%20from%202023-04-19%2021-31-32.png"  title="Kafka">
  <img src="https://github.com/Pranjal-Tripathi-01/Kafka/blob/main/Screenshot%20from%202023-04-19%2021-42-44.png"  title="Cassandra">  
</p>
