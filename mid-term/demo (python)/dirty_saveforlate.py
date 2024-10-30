import random
from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
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

# Fetch account_ID from the Account table to use as foreign keys
try:
    accounts = pd.read_sql("SELECT account_ID, create_Date FROM Account", engine)
except exc.SQLAlchemyError as e:
    print(f"Error fetching account IDs and creation dates: {e}")
    exit()

# Fetch item_ID from the Item table to use as foreign keys
try:
    item_ids = pd.read_sql("SELECT item_ID FROM Item", engine)
except exc.SQLAlchemyError as e:
    print(f"Error fetching item IDs: {e}")
    exit()

# Function to generate a unique sfl_ID using uuid4() from Faker
def generate_sfl_id():
    return str(fake.uuid4())[:10]  # Limit to 10 characters

# Function to generate save_date before account creation date
def generate_save_date(account_creation_date):
    # Generate random number of days before account creation date
    days_ago = random.randint(1, 365)
    save_date = account_creation_date - timedelta(days=days_ago)
    return save_date

# Generate 300 rows of sample data for Save_For_Later
data_rows = []
for _ in range(30):
    # Randomly select an account
    account = accounts.sample(1).iloc[0]
    account_id = account['account_ID']
    account_creation_date = account['create_Date']
    
    # Randomly select an item_ID from the list
    item_id = item_ids.sample(1).iloc[0]['item_ID']
    
    # Generate a unique sfl_ID
    sfl_id = generate_sfl_id()
    
    # Generate a random save_date that is earlier than account creation date
    save_date = generate_save_date(account_creation_date)
    
    data_row = (account_id, sfl_id, item_id, save_date)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['sfl_account_ID', 'sfl_ID', 'sfl_item_ID', 'save_Date'])

# Insert data into the 'Save_For_Later' table
try:
    df.to_sql('Save_For_Later', con=engine, if_exists='append', index=False)
except exc.SQLAlchemyError as e:
    print(f"Error inserting data into 'Save_For_Later' table: {e}")
