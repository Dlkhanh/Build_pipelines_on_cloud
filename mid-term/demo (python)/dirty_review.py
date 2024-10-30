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
    account_ids = pd.read_sql("SELECT account_ID FROM Account", engine)
except exc.SQLAlchemyError as e:
    print(f"Error fetching account IDs: {e}")
    exit()

# Fetch item_ID from the Item table to use as foreign keys
try:
    item_ids = pd.read_sql("SELECT item_ID FROM Item", engine)
except exc.SQLAlchemyError as e:
    print(f"Error fetching item IDs: {e}")
    exit()

# Function to generate a unique review_ID using uuid4() from Faker
def generate_review_id():
    return str(fake.uuid4())[:10]  # Limit to 10 characters

# Function to generate negative r_star
def generate_negative_r_star():
    return round(random.uniform(-5, -0.01), 2)  # Negative star ratings

# Function to generate comments with special characters and multiple spaces
def generate_comments():
    special_chars = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '+']
    num_chars = random.randint(50, 200)
    comments = ''.join(random.choice(special_chars + [' '] * 10) for _ in range(num_chars))
    return comments.strip()

# Function to generate reviewDate
def generate_review_date():
    days_ago = random.randint(1, 365)  # Random days ago up to one year
    return datetime.now() - timedelta(days=days_ago)

# Generate 300 rows of sample data for Review
data_rows = []
for _ in range(30):
    # Randomly select an account_ID from the list
    account_id = account_ids.sample(1).iloc[0]['account_ID']
    
    # Randomly select an item_ID from the list
    item_id = item_ids.sample(1).iloc[0]['item_ID']
    
    # Generate negative r_star
    r_star = generate_negative_r_star()
    
    # Generate comments with special characters and multiple spaces
    comments = generate_comments()
    
    # Generate a random reviewDate
    review_date = generate_review_date()
    
    # Generate random useful_flag (either 0 or 1)
    useful_flag = random.choice([0, 1])
    
    # Generate random num_of_words (between 50 and 200)
    num_of_words = random.randint(50, 200)
    
    # Generate a unique review_ID
    review_id = generate_review_id()
    
    data_row = (review_id, r_star, comments, review_date, useful_flag, num_of_words, account_id, item_id)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['review_ID', 'r_star', 'comments', 'reviewDate', 'useful_flag', 'num_of_words', 'review_account', 'review_item'])

# Insert data into the 'Review' table
try:
    df.to_sql('Review', con=engine, if_exists='append', index=False)
except exc.SQLAlchemyError as e:
    print(f"Error inserting data into 'Review' table: {e}")
