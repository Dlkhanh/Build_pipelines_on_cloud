import random
from faker import Faker
import string
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

# Generate 300 rows of sample data
data_rows = []
for _ in range(300):
    # Tạo các số nguyên ngẫu nhiên cho hai cột đầu
    item_ID = random.randint(1000000000, 9999999999)
    sku = random.randint(10000000, 99999999)
    
    name = fake.word()[:40]  # Giới hạn độ dài tối đa là 40 ký tự
    price = round(random.uniform(1, 1000), 2)  # Giá tiền ngẫu nhiên từ 1 đến 1000 với 2 chữ số sau dấu phẩy
    item_desc = fake.text(max_nb_chars=100)  # Mô tả sản phẩm ngẫu nhiên, tối đa 100 ký tự
    category = random.choice(['A', 'B', 'C', 'D', 'E'])  # Lựa chọn ngẫu nhiên một danh mục
    stock = random.randint(0, 1000)  # Số lượng tồn kho ngẫu nhiên từ 0 đến 1000
    item_star = random.randint(0, 5) if random.random() < 0.8 else None  # Đánh giá sản phẩm ngẫu nhiên từ 0 đến 5, có xác suất 80% có giá trị, 20% là None
    item_sizes = random.choice(['S', 'M', 'L', 'XL', 'XXL', 'XXXL']) if random.random() < 0.5 else None  # Kích thước sản phẩm ngẫu nhiên hoặc None
    color = fake.color_name() if random.random() < 0.7 else None  # Màu sắc ngẫu nhiên hoặc None, có xác suất 70%

    data_row = (item_ID, sku, name, price, item_desc, category, stock, item_star, item_sizes, color)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['item_ID', 'sku', 'name', 'price', 'item_desc', 'category', 'stock', 'item_star', 'item_sizes', 'color'])

# Insert data into the 'Item' table
df.to_sql('Item', con=engine, if_exists='append', index=False)
