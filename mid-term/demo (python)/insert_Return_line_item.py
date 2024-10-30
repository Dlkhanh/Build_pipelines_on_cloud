import random
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
import string

# Khởi tạo Faker để tạo dữ liệu giả
fake = Faker()

# Thông tin kết nối SQL Server
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'

# Tạo SQLAlchemy engine
connection_string = f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server'
print(f"Kết nối tới SQL Server với chuỗi kết nối: {connection_string}")

try:
    engine = create_engine(connection_string)

    # Lấy danh sách returns_ID từ bảng Returns
    returns_ids = pd.read_sql("SELECT returns_ID FROM Returns", engine)["returns_ID"].tolist()

    # Lấy order_ID, order_item_ID, và quantity từ bảng Order_Item
    order_items = pd.read_sql("SELECT order_ID, order_item_ID AS order_item, quantity FROM Order_Item", engine)

    # Tạo 300 hàng dữ liệu mẫu
    data_rows = []
    for _ in range(300):
        return_ID = random.choice(returns_ids)  # Chọn ngẫu nhiên một return_ID
        return_item_ID = ''.join(random.choices(string.digits, k=10))  # Tạo một chuỗi số ngẫu nhiên 10 chữ số cho return_item_ID

        # Chọn một order_item hợp lệ đáp ứng điều kiện số lượng yêu cầu
        valid_order_items = order_items[order_items['quantity'] >= 1]

        if valid_order_items.empty:
            continue  # Nếu không có order_item hợp lệ, bỏ qua vòng lặp này

        selected_order_item = valid_order_items.sample().iloc[0]
        order_ID = selected_order_item['order_ID']
        order_item = selected_order_item['order_item']
        quantity = selected_order_item['quantity']

        data_row = (return_ID, return_item_ID, quantity, order_item, order_ID)
        data_rows.append(data_row)

    # Chuyển đổi dữ liệu thành DataFrame
    df = pd.DataFrame(data_rows, columns=['return_ID', 'return_item_ID', 'quantity', 'order_item', 'order_ID'])

    # Chèn dữ liệu vào bảng 'Return_line_item'
    df.to_sql('Return_line_item', con=engine, if_exists='append', index=False)
    print("Chèn dữ liệu thành công.")
except Exception as e:
    print(f"Lỗi: {e}")
