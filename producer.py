
from confluent_kafka import Producer
import mysql.connector



# MySQL database configu
mysql_host = 'localhost'
mysql_user = 'pranjal_tripathi'
mysql_password = 'pranjal'
mysql_database = 'kafka'

# Kafka config
api_key = 'BWSP3QQSM5KXN6SN'
api_secret = 'vokHg2n7FlV0DKXidPn/QPM0FiZ75GkmtlJdITJ4sm+ftCuH4omZ7rQEGZweFNuG'
schema_registry_api_key = 'GKTCSYQYHDHZBDNS'
schema_registry_api_secret = 'Z8CZQgO9UddnB7Z68BpwY4fImn4bL0gZJat/MNy7Z+McB+TucGGWAwuh9gXsQ56B'
endpoint_schema_url = 'https://psrc-knmx2.australia-southeast1.gcp.confluent.cloud'
bootstrap_servers = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
security_protocol = 'SASL_SSL'
ssl_mechanism = 'PLAIN'

# Kafka topic to produce messages
kafka_topic = 'students_data'

# Kafka producer config
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanism': ssl_mechanism,
    'sasl.username': api_key,
    'sasl.password': api_secret,
}
kafka_producer = Producer(producer_config)

# Connecting to the MySQL database and creating a cursor
mysql_config = {
    'host': mysql_host,
    'user': mysql_user,
    'password': mysql_password,
    'database': mysql_database
}
mysql_connection = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_connection.cursor()

# Executing query to get data from the MySQL table
mysql_query = 'SELECT * FROM student_data'
mysql_cursor.execute(mysql_query)

# Iterating over the rows returned by the query
for row in mysql_cursor:
    # Prepareing a message with the data from the row
    message_value = {
        'id': row[0],
        'name': row[1],
        'age': row[2],
        'timestamp': str(row[3])
    }
    import json


    # encoding the dictionary object to bytes format
    msg_value = json.dumps(message_value).encode('utf-8')



    # Producing the message to the Kafka topic
    kafka_producer.produce(
        topic=kafka_topic,
        value=msg_value,
        key=str(row[0])
    )
print("Sending Messages")

# Flushing the Kafka producer 
kafka_producer.flush()

print("Messages sent successfully")


