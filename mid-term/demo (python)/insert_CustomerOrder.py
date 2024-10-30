import random
from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
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

# Tạo danh sách các account_ID và create_date từ bảng Account để sử dụng làm khóa ngoại
account_info = pd.read_sql("SELECT account_ID, create_Date FROM Account", engine)

# Generate 300 rows of sample data
data_rows = []
for _ in range(300):
    # Tạo order_ID là chuỗi ngẫu nhiên có độ dài là 10 ký tự SỐ
    order_ID = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    
    # Chọn ngẫu nhiên một dòng từ account_info
    account = account_info.sample(1).iloc[0]
    account_ID = account['account_ID']
    create_date = account['create_Date']
    
    s_street = fake.street_address()[:40]  # Địa chỉ đường phố, giới hạn độ dài tối đa là 40 ký tự
    s_City = fake.city()[:20]  # Thành phố, giới hạn độ dài tối đa là 20 ký tự
    s_State = fake.state_abbr()[:2]  # Mã bang, giới hạn độ dài là 2 ký tự
    s_Zip = fake.zipcode()[:10]  # Mã zip, giới hạn độ dài tối đa là 10 ký tự
    
    # Tạo ngẫu nhiên các ngày cho order_date, dispatch_date và delivery_date theo điều kiện đã cho
    order_date = fake.date_between_dates(date_start=create_date, date_end=datetime.now())
    dispatch_date = fake.date_between_dates(date_start=order_date + timedelta(days=1), date_end=order_date + timedelta(days=7))
    delivery_date = fake.date_between_dates(date_start=dispatch_date + timedelta(days=1), date_end=dispatch_date + timedelta(days=7))
    
    receiver_name = fake.name()[:40]  # Tên người nhận, giới hạn độ dài tối đa là 40 ký tự

    data_row = (order_ID, account_ID, s_street, s_City, s_State, s_Zip, order_date, receiver_name, delivery_date, dispatch_date)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['order_ID', 'account_ID', 's_street', 's_City', 's_State', 's_Zip', 'order_date', 'receiver_name', 'delivery_date', 'dispatch_date'])

# Insert data into the 'Customer_Order' table
df.to_sql('Customer_Order', con=engine, if_exists='append', index=False)
