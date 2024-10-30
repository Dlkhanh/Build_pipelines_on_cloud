import random
import pandas as pd
from faker import Faker
import string  # Thêm dòng này để import thư viện string
from sqlalchemy import create_engine

# Initialize Faker to generate fake data
fake = Faker()

# Thông tin kết nối SQL Server
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'


# Tạo SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Generate 300 rows of sample data
data_rows = []
for _ in range(300):
    # Tạo các số ngẫu nhiên có 16 chữ số cho cột đầu
    card_number = ''.join([str(random.randint(0, 9)) for _ in range(16)])
    
    cc_name = fake.name()[:40]  # Giới hạn độ dài tối đa là 40 ký tự
    pin = ''.join(random.choices(string.digits, k=4))  # Tạo mã pin bằng các số ngẫu nhiên có 4 chữ số
    b_street = fake.street_address()[:40]  # Địa chỉ đường phố, giới hạn độ dài tối đa là 40 ký tự
    b_city = fake.city()[:20]  # Thành phố, giới hạn độ dài tối đa là 20 ký tự
    b_state = fake.state_abbr()[:2]  # Mã bang, giới hạn độ dài là 2 ký tự
    b_zip = fake.zipcode()[:10]  # Mã zip, giới hạn độ dài tối đa là 10 ký tự

    data_row = (card_number, cc_name, pin, b_street, b_city, b_state, b_zip)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['card_number', 'cc_name', 'pin', 'b_street', 'b_city', 'b_state', 'b_zip'])

# Insert data into the 'Card' table
df.to_sql('Card', con=engine, if_exists='append', index=False)
