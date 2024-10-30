import random
import pandas as pd
from faker import Faker
import string
from sqlalchemy import create_engine

# Initialize Faker to generate fake data
fake = Faker()

# Function to generate dirty data
def generate_dirty_card_data():
    card_number = ''.join([str(random.randint(0, 9)) for _ in range(16)])
    cc_name = (fake.name() + "@" + ''.join(random.choices(string.punctuation, k=3)))[:40]  # Random special characters
    pin = ''.join(random.choices(string.digits, k=4))
    b_street = (fake.street_address() + " " + ''.join(random.choices(string.punctuation, k=2)))[:40]  # Random special characters
    b_city = (fake.city() + " " + ''.join(random.choices(string.punctuation, k=2)))[:20]  # Random special characters
    b_state = (fake.state_abbr() + "!" if len(fake.state_abbr() + "!") <= 2 else fake.state_abbr())  # Random special character
    b_zip = (fake.zipcode() + "$")[:10]  # Random special characters
    
    return {
        "card_number": card_number,
        "cc_name": cc_name,
        "pin": pin,
        "b_street": b_street,
        "b_city": b_city,
        "b_state": b_state,
        "b_zip": b_zip,
    }

# Database connection details
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Create SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Generate 30 rows of data
data_rows = [generate_dirty_card_data() for _ in range(30)]

# Convert data to DataFrame
df = pd.DataFrame(data_rows)

# Insert data into the 'Card' table using SQLAlchemy
df.to_sql('Card', con=engine, if_exists='append', index=False)
print("Data inserted successfully!")
