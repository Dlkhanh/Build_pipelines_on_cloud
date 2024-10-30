import random
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
import string
from datetime import datetime

# Initialize Faker to generate fake data
fake = Faker()

# Thông tin kết nối SQL Server
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Tạo SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Tạo danh sách các account_ID từ bảng Account và item_ID từ bảng Item để sử dụng làm khóa ngoại
accounts = pd.read_sql("SELECT account_ID, create_date FROM Account", engine)
items = pd.read_sql("SELECT item_ID FROM Item", engine)
orders = pd.read_sql("SELECT order_ID, order_date, account_ID FROM Customer_Order", engine)

# Generate 300 rows of sample data
data_rows = []
for _ in range(300):
    account = accounts.sample().iloc[0]  # Chọn ngẫu nhiên một account từ bảng Account
    account_ID = account['account_ID']
    create_date = pd.to_datetime(account['create_date'])
    
    order_dates = orders[orders['account_ID'] == account_ID]['order_date']
    if not order_dates.empty:
        order_date = pd.to_datetime(random.choice(order_dates.tolist()))
    else:
        order_date = min(datetime.today(), create_date + pd.Timedelta(days=random.randint(1, 365)))  # Giả sử order_date nếu không có đơn hàng
    
    save_date = fake.date_between(start_date=create_date, end_date=order_date)
    sfl_ID = ''.join(random.choices(string.digits, k=10))  # Tạo sfl_ID là chuỗi số ngẫu nhiên có độ dài là 10 ký tự
    item_ID = random.choice(items['item_ID'].tolist())  # Chọn ngẫu nhiên một item_ID từ danh sách

    data_row = (account_ID, sfl_ID, item_ID, save_date)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['sfl_account_ID', 'sfl_ID', 'sfl_item_ID', 'save_Date'])

# Insert data into the 'Save_For_Later' table
df.to_sql('Save_For_Later', con=engine, if_exists='append', index=False)

# Chạy một truy vấn SQL
query = "SELECT * FROM Save_For_Later"
df = pd.read_sql(query, engine)

# Hiển thị DataFrame
print(df.head())
