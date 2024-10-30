import random
from faker import Faker
import pandas as pd
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

# Tạo danh sách các order_ID từ bảng Customer_Order để sử dụng làm khóa ngoại
order_ids = pd.read_sql("SELECT order_ID FROM Customer_Order", engine)

# Tạo danh sách các item_ID từ bảng Item để sử dụng làm khóa ngoại
item_ids = pd.read_sql("SELECT item_ID FROM Item", engine)

# Generate 300 rows of sample data for Order_Item
data_rows = []
for _ in range(30):
    # Chọn ngẫu nhiên một order_ID từ danh sách
    order_id = order_ids.sample(1).iloc[0]['order_ID']
    
    # Tạo order_item_ID là chuỗi ngẫu nhiên có độ dài là 10 ký tự SỐ
    order_item_id = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    
    # Chọn ngẫu nhiên một item_ID từ danh sách
    item_id = item_ids.sample(1).iloc[0]['item_ID']
    
    # Giá giảm, giá trị ngẫu nhiên từ -500 đến 500
    discounted_price = round(random.uniform(-500.0, 0), 2)
    
    # Số lượng ngẫu nhiên từ -20 đến 20
    quantity = random.randint(-20, -1)

    data_row = (order_id, order_item_id, item_id, discounted_price, quantity)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['order_ID', 'order_item_ID', 'item_ID', 'discounted_price', 'quantity'])

# Insert data into the 'Order_Item' table
df.to_sql('Order_Item', con=engine, if_exists='append', index=False)
