import random
from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Initialize Faker to generate fake data
fake = Faker()

# SQL Server connection details
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Create SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Fetch order_ID from the Customer_Order table to use as foreign keys
order_ids = pd.read_sql("SELECT order_ID FROM Customer_Order", engine)

# Function to generate a status timestamp
def generate_status_timestamp(base_date):
    return (base_date + timedelta(days=random.randint(1, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))).strftime('%Y-%m-%d %H:%M:%S')

# Generate 300 rows of sample data for ORDER_STATUS
data_rows = []
for _ in range(30):
    # Randomly select an order_ID from the list
    order_id = order_ids.sample(1).iloc[0]['order_ID']
    
    # Generate a random status timestamp based on the current time
    status_timestamp = generate_status_timestamp(datetime.now())

    # Use None as the default status value
    status = None

    data_row = (order_id, status, status_timestamp)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['status_Order_ID', 'status', 'status_Timestamp'])

# Insert data into the 'ORDER_STATUS' table
df.to_sql('ORDER_STATUS', con=engine, if_exists='append', index=False)
