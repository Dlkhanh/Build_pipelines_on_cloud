import random
from faker import Faker
import pandas as pd
from datetime import datetime
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

# Fetch card_number from the Card table to use as foreign keys
card_numbers = pd.read_sql("SELECT card_number FROM Card", engine)

# Function to generate a random payment date (optional)
def generate_payment_date():
    return fake.date_this_year()

# Generate 300 rows of sample data for Payment
data_rows = []
for _ in range(30):  # Increased the number of rows to 300
    # Randomly select an order_ID from the list
    order_id = order_ids.sample(1).iloc[0]['order_ID']
    
    # Randomly select a card number from the list
    card_n = card_numbers.sample(1).iloc[0]['card_number']
    
    # Randomly generate negative total_amount
    total_amount = round(random.uniform(-1000, -0.01), 2)  # Negative total amount

    # Randomly generate payment_date; sometimes it's None
    payment_date = generate_payment_date() if random.choice([True, False]) else None

    # Generate a random UUID string and truncate to fit VARCHAR(10)
    uuid_str = str(fake.uuid4())
    payment_id = uuid_str[:10]  # Limit to 10 characters if UUID is longer
    
    data_row = (payment_id, order_id, payment_date, total_amount, card_n)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['payment_id', 'order_id', 'payment_date', 'total_amount', 'card_n'])

# Insert data into the 'Payment' table
df.to_sql('Payment', con=engine, if_exists='append', index=False)
