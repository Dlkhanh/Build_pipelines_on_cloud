import random
import pandas as pd
from faker import Faker
import string
from sqlalchemy import create_engine

# Khởi tạo Faker để tạo dữ liệu giả
fake = Faker()

# Thông tin kết nối SQL Server
sql_server_host = 'INTERN-DLKHANH-\\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Tạo SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Hàm tạo dữ liệu bẩn cho bảng Account
def generate_dirty_data_account(num_records):
    data_rows = []
    for _ in range(num_records):
        account_ID = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        registration_ID = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        fName = fake.first_name()
        lName = fake.last_name()
        email = fake.email()
        cell_Num = ''.join(random.choices(string.digits, k=12)) if random.choice([True, False]) else None
        logon_ID = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
        password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=32))
        create_Date = fake.date_this_decade()
        account_Type = random.choice(['P', 'B'])  # Chỉ chọn các giá trị hợp lệ

        # Thêm một số dữ liệu hoàn toàn không hợp lệ để kiểm tra
        if random.choice([True, False]):
            fName = ''.join(random.choices(string.ascii_uppercase + string.digits + string.punctuation, k=20))
        if random.choice([True, False]):
            email = ''.join(random.choices(string.ascii_uppercase + string.digits, k=50))  # Giới hạn email để không vượt quá giới hạn SQL Server

        data_row = (account_ID, registration_ID, fName, lName, email, cell_Num, logon_ID, password, create_Date, account_Type)
        data_rows.append(data_row)

    df = pd.DataFrame(data_rows, columns=['account_ID', 'registration_ID', 'fName', 'lName', 'email', 'cell_Num', 'logon_ID', 'password', 'create_Date', 'account_Type'])
    df.to_sql('Account', con=engine, if_exists='append', index=False)

# Tạo 50 dòng dữ liệu bẩn cho bảng Account
generate_dirty_data_account(30)
