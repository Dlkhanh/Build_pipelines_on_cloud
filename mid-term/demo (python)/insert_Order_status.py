from sqlalchemy import create_engine
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

# Khởi tạo đối tượng Faker
fake = Faker()

# Kết nối đến cơ sở dữ liệu SQL Server
sql_server_host = 'INTERN-DLKHANH-\LEKHANH'
sql_server_database = 'demo'
sql_server_user = 'sa'
sql_server_password = '0909'
# Tạo SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{sql_server_user}:{sql_server_password}@{sql_server_host}/{sql_server_database}?driver=ODBC+Driver+17+for+SQL+Server')

# Lấy danh sách order_ID và các ngày liên quan từ bảng Customer_Order
customer_orders = pd.read_sql('SELECT order_ID, order_date, dispatch_date, delivery_date FROM Customer_Order WHERE delivery_date < GETDATE()', con=engine)

# Lấy danh sách các bản ghi hiện có trong bảng ORDER_STATUS để tránh trùng lặp
existing_order_statuses = pd.read_sql('SELECT status_Order_ID, status_Timestamp FROM ORDER_STATUS', con=engine)
existing_records = set(zip(existing_order_statuses['status_Order_ID'], existing_order_statuses['status_Timestamp']))

# Tạo DataFrame chứa dữ liệu cho bảng ORDER_STATUS
order_status_data = {
    'status_Order_ID': [],
    'status': [],
    'status_Timestamp': []
}

# Tạo dữ liệu ngẫu nhiên cho bảng ORDER_STATUS
for _, order in customer_orders.iterrows():
    order_ID = order['order_ID']
    order_date = order['order_date']
    dispatch_date = order['dispatch_date']
    delivery_date = order['delivery_date']
    
    # Generate status timestamps in sequence
    processing_timestamp = order_date + timedelta(days=random.uniform(0, 1))
    shipped_timestamp = processing_timestamp + timedelta(days=random.uniform(1, 2))
    out_for_delivery_timestamp = shipped_timestamp + timedelta(days=random.uniform(1, (delivery_date - shipped_timestamp).days))
    delivered_timestamp = delivery_date

    statuses = [
        ('Processing', processing_timestamp),
        ('Shipped', shipped_timestamp),
        ('Out For Delivery', out_for_delivery_timestamp),
        ('Delivered', delivered_timestamp)
    ]

    for status, timestamp in statuses:
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        timestamp = datetime(timestamp.year, timestamp.month, timestamp.day, random_hour, random_minute, random_second)
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        existing_record = (order_ID, timestamp_str)
        
        if existing_record in existing_records:
            continue  # Skip this iteration if the record already exists
        else:
            existing_records.add(existing_record)

        order_status_data['status_Order_ID'].append(order_ID)
        order_status_data['status'].append(status)
        order_status_data['status_Timestamp'].append(timestamp_str)

# Tạo DataFrame từ dữ liệu
order_status_df = pd.DataFrame(order_status_data)

# Chèn dữ liệu vào bảng ORDER_STATUS
order_status_df.to_sql('ORDER_STATUS', con=engine, if_exists='append', index=False)
