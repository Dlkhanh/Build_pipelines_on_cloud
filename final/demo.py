# Databricks notebook source
# MAGIC %pip install requests
# MAGIC %pip install fuzzywuzzy
# MAGIC %pip install python-Levenshtein
# MAGIC

# COMMAND ----------

# Đọc file .csv 
df_books= spark.read.csv('dbfs:/FileStore/end_of_term/books.csv', header=True, inferSchema=True, sep=';',encoding='iso-8859-1')
df_ratings= spark.read.csv('dbfs:/FileStore/end_of_term/ratings.csv', header=True, inferSchema=True, sep=';',encoding='iso-8859-1')
df_users= spark.read.csv('dbfs:/FileStore/end_of_term/users.csv', header=True, inferSchema=True, sep=';',encoding='iso-8859-1')

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F
# cột Age chỉ chứa số 
df_users = df_users.withColumn("Age", F.regexp_extract("Age", r"\d+", 0))
# Chuyển đổi kiểu dữ liệu 
df_ratings = df_ratings.withColumn("User-ID", col("User-ID").cast("string"))
df_users = df_users.withColumn("Age", col("Age").cast("integer"))

# COMMAND ----------

display(df_books)
display(df_ratings)
display(df_users)

# COMMAND ----------

print(f"df_books: {df_books.count()}")
print(f"df_ratings: {df_ratings.count()}")
print(f"df_users: { df_users.count()}")

# COMMAND ----------

df_books = df_books.dropDuplicates(["ISBN","Book-Title"])
df_users= df_users.dropDuplicates(["User-ID"])
df_ratings = df_ratings.dropDuplicates(["ISBN", "User-ID"])

# COMMAND ----------

print(f"df_books: {df_books.count()}")
print(f"df_ratings: {df_ratings.count()}")
print(f"df_users: { df_users.count()}")

# COMMAND ----------

# Làm sạch cột Book-Title bằng cách giữ lại các ký tự số, chữ cái, khoảng trắng, và một số ký tự đặc biệt.
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StringType

# Định nghĩa hàm làm sạch các cột string trừ cột "Location" và các cột chứa "URL"
def clean_all_string_columns_except_location(df):
    string_columns = [col_name for col_name, col_type in df.dtypes if col_type == "string" and col_name != "Location" and "URL" not in col_name]
    for column_name in string_columns:
        df = df.withColumn(column_name, regexp_replace(col(column_name), r"[^0-9a-zA-Z\s\.\(\)]", ""))
    return df

# Áp dụng hàm làm sạch cho các DataFrame
df_books = clean_all_string_columns_except_location(df_books)
df_users = clean_all_string_columns_except_location(df_users)
df_ratings = clean_all_string_columns_except_location(df_ratings)

#xóa chữ số trong các cột bảng Books
df_books = df_books.withColumn("Book-Author", regexp_replace(col("Book-Author"), "[^a-zA-Z\s\.\(\)]", ""))
df_books = df_books.withColumn("Publisher", regexp_replace(col("Publisher"), "[^a-zA-Z\s\.\(\)]", ""))

# Hiển thị kết quả để kiểm tra
display(df_books)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
df_users = df_users.withColumn("Location", regexp_replace(col("Location"), "[^a-zA-Z\s,]", ""))

# COMMAND ----------

display(df_users)

# COMMAND ----------

from pyspark.sql.functions import col, split, reverse, expr

# Đảo ngược chuỗi trong cột Location, tách thành hai cột và đảo ngược lại giá trị gốc
df_users = df_users.withColumn("Reversed_Location", expr("reverse(Location)")) \
                   .withColumn("Reversed_Split", split(col("Reversed_Location"), ",\s*")) \
                   .withColumn("Country", expr("reverse(Reversed_Split[0])")) \
                   .withColumn("City", expr("reverse(Reversed_Split[1])")) \
                   .drop("Reversed_Location", "Reversed_Split")

# Hiển thị dữ liệu đã xử lý
display(df_users)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Định nghĩa cửa sổ phân vùng theo thành phố
window_spec = Window.partitionBy("City").orderBy(F.col("Country").desc_nulls_last())

# Điền các ô quốc gia rỗng dựa trên thành phố đã có quốc gia
df_users = df_users.withColumn("Country", F.when(
    (F.col("Country").isNull() | (F.col("Country") == "")),
    F.first("Country", ignorenulls=True).over(window_spec)
).otherwise(F.col("Country")))

# Hiển thị DataFrame kết quả
display(df_users)


# COMMAND ----------

import requests
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from fuzzywuzzy import process
# Gọi API để lấy danh sách các quốc gia
response = requests.get('https://restcountries.com/v3.1/all')
countries = [country['name']['common'] for country in response.json()]
# Tạo hàm so khớp mờ (fuzzy matching)
def fuzzy_match(country):
    match, score = process.extractOne(country, countries)
    return match if score >= 80 else country
# Đăng ký hàm UDF
fuzzy_match_udf = udf(fuzzy_match, StringType())
# Áp dụng hàm UDF cho cột Country
df_users = df_users.withColumn("Country", fuzzy_match_udf(col("Country")))

# Hiển thị kết quả để kiểm tra
display(df_users)

# COMMAND ----------

#Giữ những dòng có ít nhất chứa 1 kí tự chữ cái (in hoa hoặc in thường)
from pyspark.sql.functions import col
df_users = df_users.filter(col("Country").rlike("[a-zA-Z]"))
# print(df_users.count())

# COMMAND ----------

# display(df_users)

# COMMAND ----------

from pyspark.sql import functions as F

def process_null_values(df, column_name, condition):
    total_count = df.count()
    null_count = df.filter(condition).count()
    null_percentage = null_count / total_count * 100
    
    if null_percentage > 10:
        # Thay thế giá trị null/hợp lệ bằng giá trị trung vị
        median_value = df.approxQuantile(column_name, [0.5], 0.25)[0]  # Sử dụng median thay vì mean
        df = df.withColumn(column_name, F.when(condition, median_value).otherwise(F.col(column_name)))
    
    # Loại bỏ các hàng chứa giá trị null/hợp lệ
    df = df.filter(~condition)
    
    return df

# Xử lý cột Age
age_condition = (F.col("Age").isNull()) | (F.col("Age") < 12) | (F.col("Age") > 80)
df_users = process_null_values(df_users, "Age", age_condition)

# Xử lý cột Year-Of-Publication
year_condition = (F.col("Year-Of-Publication").isNull()) | (F.col("Year-Of-Publication") == 0) | (F.col("Year-Of-Publication") > 2024)
df_books = process_null_values(df_books, "Year-Of-Publication", year_condition)
display(df_users)
display(df_books)


# COMMAND ----------

from pyspark.sql import functions as F

# Xóa các bản ghi không hợp lệ dựa trên ISBN từ df_ratings
df_ratings_valid_isbn = df_ratings.alias("ratings").join(df_books.alias("books"), F.col("ratings.ISBN") == F.col("books.ISBN"), how="inner")

# Xóa các bản ghi không hợp lệ dựa trên User-ID từ df_ratings
df_ratings_valid = df_ratings_valid_isbn.join(df_users.alias("users"), F.col("ratings.User-ID") == F.col("users.User-ID"), how="inner")

# Chọn các cột cần thiết từ df_ratings_cleaned và tránh lỗi "AMBIGUOUS_REFERENCE"
df_ratings_cleaned = df_ratings_valid.select(
    F.col("ratings.User-ID").alias("UserID"),
    F.col("ratings.ISBN"),
    F.col("ratings.Book-Rating")
)

# Tạo lại các DataFrames riêng biệt sau khi lọc các bản ghi không hợp lệ
df_books_cleaned = df_books
df_users_cleaned = df_users
df_ratings_cleaned = df_ratings_cleaned  # Sử dụng df_ratings_cleaned đã được xử lý

# COMMAND ----------

print(f"df_books: {df_ratings_cleaned.count()}")


# COMMAND ----------

from pyspark.sql.functions import col
# Loại bỏ cột không cần thiết
df_users_cleaned=df_users_cleaned.drop("Location","City")
display(df_users_cleaned)
# lấy những cột cần trong bảng Books
df_books_cleaned=df_books_cleaned.drop("Image-URL-S","Image-URL-M","Image-URL-L")
display(df_books_cleaned)


