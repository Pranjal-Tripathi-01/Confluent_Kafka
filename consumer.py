from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from confluent_kafka import Consumer, KafkaError




# Kafka consumer config
api_key = 'BWSP3QQSM5KXN6SN'
api_secret = 'vokHg2n7FlV0DKXidPn/QPM0FiZ75GkmtlJdITJ4sm+ftCuH4omZ7rQEGZweFNuG'
schema_registry_api_key = 'GKTCSYQYHDHZBDNS'
schema_registry_api_secret = 'Z8CZQgO9UddnB7Z68BpwY4fImn4bL0gZJat/MNy7Z+McB+TucGGWAwuh9gXsQ56B'
endpoint_schema_url = 'https://psrc-knmx2.australia-southeast1.gcp.confluent.cloud'
bootstrap_servers = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
security_protocol = 'SASL_SSL'
ssl_mechanism = 'PLAIN'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanism': ssl_mechanism,
    'sasl.username': api_key,
    'sasl.password': api_secret,
}

# Cassandra config
cloud_config= {
         'secure_connect_bundle': 'secure-connect-kafka-project.zip'
}
auth_provider = PlainTextAuthProvider('cgsBsKEvBjOuRiiErXWygacN', 
                            'ua3dCXMp0A_QbPnUZ.IKrz88XR0T6aJgb68lRgv.sD,GMD357gXETEDtg_-rG98d58TLsFlFudgTrC0WkE7wtgiJKZkOnS8Xp7.KiiaTgYNSHEDW69pQF,ZStuR4EEKl')


keyspace = 'students_data'
table_name = 'students_data'

# Kafka topic and Cassandra column names
topic = 'students_data'
columns = ['id', 'name', 'age', 'timestamp']

# Creating a Kafka consumer
consumer_config.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})
consumer = Consumer(consumer_config)
consumer.subscribe([topic])

# Creating a Cassandra session
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

try:
    query1 = "use students_data"
    session.execute(query1)
    print("Inside the  keyspace")
    query2 = """create table students_data(
                id int,
                name varchar,
                age int,
                timestamp varchar,
                primary key(id)
              )
            """
    session.execute(query2)
    print("Table created inside the kafka_project keyspace")
except Exception as err:
    print("Exception Occured while using Keyspace : ",err)

# Preparing a Cassandra insert statement
insert_stmt = session.prepare(f"INSERT INTO {table_name} (id, name, age, timestamp) VALUES (?, ?, ?, ?)")

# Consuming messages from Kafka and writing them to Cassandra
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f'End of partition reached: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}')
        else:
            print(f'Error while consuming message: {msg.error()}')
    else:
        # Parsing the message value as a dictionary
        data = eval(msg.value().decode('utf-8'))

        # Writing the data to Cassandra
        print("writting messages into cassandra")
        session.execute(insert_stmt, (data[columns[0]], data[columns[1]], data[columns[2]]*2, data[columns[3]]))

