import pandas as pd
from sqlalchemy import create_engine
import random
import string
from faker import Faker

# Khởi tạo Faker để tạo dữ liệu giả
fake = Faker()

# Thông tin kết nối SQL Server
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Tạo SQLAlchemy engine
connection_string = f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(connection_string)

# Đọc dữ liệu từ SQL Server
order_items = pd.read_sql("SELECT order_ID, CAST(discounted_price AS DECIMAL(12,2)) as discounted_price, CAST(quantity AS INT) as quantity FROM Order_Item", engine)
customer_orders = pd.read_sql("SELECT order_ID, CAST(order_date AS DATE) as order_date FROM Customer_Order", engine)
card_numbers = pd.read_sql("SELECT card_number FROM Card", engine)

# Tạo một dictionary để lưu tổng số tiền cho từng order_ID
order_total_amount = {}

# Tính toán tổng giá trị cho từng đơn hàng
for order_id, group in order_items.groupby('order_ID'):
    total_amount = (group['discounted_price'] * group['quantity']).sum()
    order_total_amount[order_id] = total_amount

# Lọc danh sách order_id đã có trong Customer_Order
valid_order_ids = customer_orders['order_ID'].unique()

# Danh sách dữ liệu Payment
data_rows = []

# Sinh dữ liệu Payment cho order_id có trong Customer_Order
for _ in range(300):
    # Chọn ngẫu nhiên order_id từ valid_order_ids
    order_id = random.choice(valid_order_ids)

    # Nếu order_id tồn tại trong dictionary tổng tiền
    if order_id in order_total_amount:
        # Lấy payment_date tương ứng với order_id
        payment_date = customer_orders[customer_orders['order_ID'] == order_id]['order_date'].values[0]

        # Lấy tổng tiền cho order_id
        total_amount = order_total_amount[order_id]

        # Lựa chọn ngẫu nhiên card_number từ danh sách
        card_number = random.choice(card_numbers['card_number'].tolist())  

        # Tạo bản ghi dữ liệu
        payment_id = ''.join(random.choices(string.digits, k=10))  # Tạo payment_id ngẫu nhiên
        data_row = (payment_id, order_id, payment_date, total_amount, card_number)
        data_rows.append(data_row)

# Chuyển danh sách dữ liệu thành DataFrame
df = pd.DataFrame(data_rows, columns=['payment_id', 'order_id', 'payment_date', 'total_amount', 'card_n'])

# Chèn dữ liệu vào bảng 'Payment' trên SQL Server
df.to_sql('Payment', con=engine, if_exists='append', index=False)

# Chạy một truy vấn SQL để xác minh dữ liệu đã được chèn thành công
query = "SELECT * FROM Payment"
df_check = pd.read_sql(query, engine)

# Hiển thị DataFrame
print(df_check.head())
