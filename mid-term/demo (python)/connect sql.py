import pandas as pd
from sqlalchemy import create_engine

# Thông tin kết nối SQL Server
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Tạo SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Kiểm tra kết nối
print(engine.connect())

# Chạy một truy vấn SQL
query = "SELECT * FROM Account"
df = pd.read_sql(query, engine)

# Hiển thị DataFrame
print(df.head())