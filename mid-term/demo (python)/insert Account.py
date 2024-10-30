import random
from faker import Faker
import sqlalchemy
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

# Initialize Faker to generate fake data
fake = Faker()

# Thông tin kết nối SQL Server
sql_server_host = 'INTERN-DLKHANH-\\LEKHANH'
sql_server_database = 'demo'
sql_server_user = ''
sql_server_password = ''
# Tạo SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Tính toán khoảng thời gian trong vòng 3 năm trước từ ngày hôm nay
end_date = datetime.now()
start_date = end_date - timedelta(days=3*365)

# Generate 300 rows of sample data
data_rows = []
for _ in range(300):
    # Tạo các số nguyên cho hai cột đầu
    account_ID = random.randint(1000000000, 9999999999)
    registration_ID = random.randint(1000000000, 9999999999)
    fName = fake.first_name()[:20]  # Giới hạn độ dài tối đa là 20 ký tự
    lName = fake.last_name()[:40]  # Giới hạn độ dài tối đa là 40 ký tự
    email = fake.email()[:320]  # Giới hạn độ dài tối đa là 320 ký tự
    # Tạo số điện thoại có đúng 10 chữ số
    cell_Num = ''.join(random.choices('0123456789', k=10))  # Giới hạn độ dài chính xác là 10 ký tự
    logon_ID = fake.user_name()[:20]  # Giới hạn độ dài tối đa là 20 ký tự
    password = fake.password(length=10)
    create_Date = fake.date_between_dates(date_start=start_date, date_end=end_date)
    account_Type = random.choice(['P', 'B'])
    
    data_row = (account_ID, registration_ID, fName, lName, email, cell_Num, logon_ID, password, create_Date, account_Type)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['account_ID', 'registration_ID', 'fName', 'lName', 'email', 'cell_Num', 'logon_ID', 'password', 'create_Date', 'account_Type'])

# Insert data into the 'Account' table
df.to_sql('Account', con=engine, if_exists='append', index=False)

# Chạy một truy vấn SQL
query = "SELECT * FROM Account"
df = pd.read_sql(query, engine)

# Hiển thị DataFrame
print(df.head())
