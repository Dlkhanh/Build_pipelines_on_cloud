import random
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
import string
from datetime import date

# Initialize Faker to generate fake data
fake = Faker()

# SQL Server connection information
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Create SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Fetch order IDs and delivery dates from Customer_Order, card numbers from Card, and payment total amounts from Payment for foreign keys
customer_orders = pd.read_sql("SELECT order_ID, delivery_date FROM Customer_Order", engine)
card_numbers = pd.read_sql("SELECT card_number FROM Card", engine)["card_number"].tolist()
payment_total_amounts = pd.read_sql("SELECT order_id, total_amount FROM Payment", engine).set_index("order_id").to_dict()["total_amount"]

order_ids = customer_orders["order_ID"].tolist()
delivery_dates = customer_orders.set_index("order_ID").to_dict()["delivery_date"]

# Generate 300 rows of sample data
data_rows = []
for _ in range(300):
    returns_ID = ''.join(random.choices(string.digits, k=10))  # Generate returns_ID as a random 10-digit string
    refund_amount = 0  # Initialize refund_amount to 0

    order_ID = random.choice(order_ids)  # Randomly select an order_ID from the list

    # Ensure refund_amount matches the total_amount from the Payment table if available
    if order_ID in payment_total_amounts:
        refund_amount = payment_total_amounts[order_ID]

    delivery_date = delivery_dates[order_ID]  # Get the delivery date for the selected order_ID

    # Ensure delivery_date is before today's date
    if delivery_date < date.today():
        # Generate a returns_date that is after the delivery_date
        returns_date = fake.date_between(start_date=delivery_date, end_date='today')
    else:
        # Skip this iteration if delivery_date is not valid
        continue

    card_n = random.choice(card_numbers)  # Randomly select a card_number from the list

    data_row = (returns_ID, refund_amount, returns_date, order_ID, card_n)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['returns_ID', 'refund_amount', 'returns_date', 'order_ID', 'card_n'])

# Insert data into the 'Returns' table
df.to_sql('Returns', con=engine, if_exists='append', index=False)
