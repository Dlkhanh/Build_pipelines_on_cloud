import random
from faker import Faker
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, exc

fake = Faker()

# SQL Server connection information
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Create SQLAlchemy engine
try:
    engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')
except exc.SQLAlchemyError as e:
    print(f"Error connecting to SQL Server: {e}")
    exit()

# Fetch order_ID from the Customer_Order table to use as foreign keys
try:
    order_ids = pd.read_sql("SELECT order_ID FROM Customer_Order", engine)
except exc.SQLAlchemyError as e:
    print(f"Error fetching order IDs: {e}")
    exit()

# Fetch card_number from the Card table to use as foreign keys
try:
    card_numbers = pd.read_sql("SELECT card_number FROM Card", engine)
except exc.SQLAlchemyError as e:
    print(f"Error fetching card numbers: {e}")
    exit()

# Function to generate a random returns date
def generate_returns_date():
    return fake.date_this_year()

# Generate 300 rows of sample data for Returns
data_rows = []
for _ in range(30):
    # Randomly select an order_ID from the list
    order_id = order_ids.sample(1).iloc[0]['order_ID']
    
    # Randomly select a card number from the list
    card_n = card_numbers.sample(1).iloc[0]['card_number']
    
    # Randomly generate negative refund amount
    refund_amount = round(random.uniform(-1000, -0.01), 2)  # Negative refund amount

    # Generate a unique identifier using uuid1() and convert to string
    returns_ID = str(fake.uuid4())[:10]  # Limit to 10 characters
    
    # Generate a random returns_date
    returns_date = generate_returns_date()
    
    data_row = (returns_ID, refund_amount, returns_date, order_id, card_n)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['returns_ID', 'refund_amount', 'returns_date', 'order_ID', 'card_n'])

# Insert data into the 'Returns' table
try:
    df.to_sql('Returns', con=engine, if_exists='append', index=False)
except exc.SQLAlchemyError as e:
    print(f"Error inserting data into 'Returns' table: {e}")
