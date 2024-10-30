import random
import pandas as pd
from faker import Faker
import string
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
order_ids = pd.read_sql("SELECT order_ID FROM Customer_Order", engine)["order_ID"].tolist()

# Tạo danh sách các item_ID và lấy giá trị price và stock từ bảng Item để sử dụng làm khóa ngoại
item_info = pd.read_sql("SELECT item_ID, price, stock FROM Item", engine)

# Generate 300 rows of sample data
data_rows = []
for _ in range(300):
    order_ID = random.choice(order_ids)
    order_item_ID = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    
    # Chọn ngẫu nhiên một dòng từ item_info
    item = item_info.sample(1).iloc[0]
    item_ID = item['item_ID']
    max_price = item['price']
    max_stock = item['stock']
    
    # Kiểm tra nếu max_stock <= 0, bỏ qua mặt hàng này
    if max_stock <= 0:
        continue
    
    discounted_price = round(random.uniform(1, max_price), 2)
    quantity = random.randint(1, max_stock)
    
    data_row = (order_ID, order_item_ID, item_ID, discounted_price, quantity)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['order_ID', 'order_item_ID', 'item_ID', 'discounted_price', 'quantity'])

# Insert data into the 'Order_Item' table
df.to_sql('Order_Item', con=engine, if_exists='append', index=False)
