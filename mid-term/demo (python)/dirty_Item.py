import pandas as pd
from sqlalchemy import create_engine
import random
from faker import Faker
import uuid

# Create a Faker instance to generate fake data
fake = Faker()

# SQL Server connection information
sql_server_host = 'INTERN-DLKHANH-\\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Create SQLAlchemy engine
database_url = f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(database_url)

# Function to generate dirty data for the 'Item' table
def generate_dirty_data_item(num_rows):
    data = []
    for _ in range(num_rows):
        item_id = str(uuid.uuid4()).replace('-', '')[:10].upper()
        sku = str(fake.random_int(10000000, 99999999))
        name = fake.word().capitalize()
        price = round(random.uniform(5, 500), 2)
        item_desc = fake.text(max_nb_chars=100)
        category = random.choice(['Electronics', 'Clothing', 'Home', 'Beauty', 'Toys'])[:10]  # Truncate to max 10 characters
        stock = random.randint(1, 1000)
        item_star = random.randint(1, 5) if random.random() > 0.1 else None  # Some may not have a rating
        item_sizes = random.choice(['S', 'M', 'L', 'XL', None])  # Some sizes may be missing
        color = random.choice(['Red', 'Blue', 'Green', 'Black', 'White', None])

        data.append([item_id, sku, name, price, item_desc, category, stock, item_star, item_sizes, color])

    return pd.DataFrame(data, columns=['item_ID', 'sku', 'name', 'price', 'item_desc', 'category', 'stock', 'item_star', 'item_sizes', 'color'])

# Generate 50 rows of dirty data
df = generate_dirty_data_item(30)

# Write data into SQL Server database
try:
    df.to_sql('Item', con=engine, if_exists='append', index=False)
    print("Data inserted successfully.")
except Exception as e:
    print(f"An error occurred while inserting data: {e}")
