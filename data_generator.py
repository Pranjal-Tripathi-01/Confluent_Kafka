import mysql.connector
import random
import datetime

# MySQL config
db = mysql.connector.connect(
    host="localhost",
    user="pranjal_tripathi",
    password="pranjal",
    database="kafka"
)

# cursor object to execute SQL queries
cursor = db.cursor()


# Creating table
cursor.execute("CREATE TABLE IF NOT EXISTS student_data ( id INT PRIMARY KEY, name VARCHAR(255), age INT, Timestamp VARCHAR(255))")
print("table created")


# Function to generate dummy records

def generate_records():
    records = []
    print('generating records')
    for i in range(20):
        id = i
        name = f"Name {i}"
        age = random.randint(18, 60) 
        timestamp= datetime.datetime.now()
        records.append((id, name, age, timestamp ))
    return records

# Inserting dummy records in a MySQL table in an infinite loop
while True:
    # Generate records
    records = generate_records()
    
    # Inserting records in the MySQL table
    sql = "INSERT INTO student_data (ID, Name, Age, Timestamp) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql, records)
    db.commit()
    print(f"{cursor.rowcount} record(s) inserted.")
