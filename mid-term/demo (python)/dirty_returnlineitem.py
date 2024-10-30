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

# Fetch return_ID from the Returns table
try:
    return_ids = pd.read_sql("SELECT returns_ID FROM Returns", engine)
except exc.SQLAlchemyError as e:
    print(f"Error fetching return IDs: {e}")
    exit()

# Fetch order_ID and order_item from the Order_Item table
try:
    order_items = pd.read_sql("SELECT order_ID, order_item_ID FROM Order_Item", engine)
except exc.SQLAlchemyError as e:
    print(f"Error fetching order items: {e}")
    exit()

# Function to generate a unique return_item_ID using uuid4() from Faker
def generate_return_item_id():
    return str(fake.uuid4())[:10]  # Limit to 10 characters

# Generate 300 rows of sample data for Return_line_item
data_rows = []
for _ in range(30):
    # Randomly select a return_ID from the list
    return_id = return_ids.sample(1).iloc[0]['returns_ID']
    
    # Randomly select an order_ID and order_item from the list
    order_item = order_items.sample(1).iloc[0]
    order_id = order_item['order_ID']
    order_item_id = order_item['order_item_ID']
    
    # Randomly generate negative quantity
    quantity = random.randint(-10, -1)  # Negative quantity

    # Generate a unique identifier for return_item_ID
    return_item_id = generate_return_item_id()
    
    data_row = (return_id, return_item_id, quantity, order_item_id, order_id)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['return_ID', 'return_item_ID', 'quantity', 'order_item', 'order_ID'])

# Insert data into the 'Return_line_item' table
try:
    df.to_sql('Return_line_item', con=engine, if_exists='append', index=False)
except exc.SQLAlchemyError as e:
    print(f"Error inserting data into 'Return_line_item' table: {e}")
