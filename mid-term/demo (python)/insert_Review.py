import random
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
import string
from datetime import timedelta

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
account_ids = pd.read_sql("SELECT account_ID FROM Account", engine)["account_ID"].tolist()
item_ids = pd.read_sql("SELECT item_ID FROM Item", engine)["item_ID"].tolist()

# Lấy thông tin delivery_date từ bảng Customer_Order
customer_orders = pd.read_sql("SELECT order_ID, delivery_date FROM Customer_Order", engine)

# Generate 300 rows of sample data
data_rows = []
for _ in range(300):
    review_ID = ''.join(random.choices(string.digits, k=10))  # Tạo review_ID là chuỗi số ngẫu nhiên có độ dài là 10 ký tự
    r_star = round(random.uniform(1, 5), 2)  # Đánh giá ngẫu nhiên từ 1 đến 5 với 2 chữ số sau dấu phẩy
    comments = fake.sentence(nb_words=random.randint(10, 50))[:255] # Bình luận ngẫu nhiên, tối đa 255 ký tự
    useful_flag = random.choice([0, 1])  # Cờ hữu ích ngẫu nhiên, 0 hoặc 1
    num_of_words = len(comments.split())  # Số lượng từ trong bình luận
    review_account = random.choice(account_ids)  # Chọn ngẫu nhiên một account_ID từ danh sách
    review_item = random.choice(item_ids)  # Chọn ngẫu nhiên một item_ID từ danh sách

    # Chọn ngẫu nhiên một đơn hàng để lấy delivery_date
    selected_order = customer_orders.sample().iloc[0]
    delivery_date = pd.to_datetime(selected_order['delivery_date'])

    # Tạo reviewDate thỏa mãn điều kiện
    min_review_date = delivery_date + timedelta(days=1)
    max_review_date = delivery_date + timedelta(days=7)
    reviewDate = fake.date_between(start_date=min_review_date, end_date=max_review_date)

    data_row = (review_ID, r_star, comments, reviewDate, useful_flag, num_of_words, review_account, review_item)
    data_rows.append(data_row)

# Convert data to DataFrame
df = pd.DataFrame(data_rows, columns=['review_ID', 'r_star', 'comments', 'reviewDate', 'useful_flag', 'num_of_words', 'review_account', 'review_item'])

# Insert data into the 'Review' table
df.to_sql('Review', con=engine, if_exists='append', index=False)
