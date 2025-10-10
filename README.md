# Real-Time ETL với Spark SQL và DataFrame

## Mục tiêu
Dự án này minh họa cách triển khai một pipeline **Real-Time ETL** (Extract - Transform - Load) sử dụng **Apache Spark Streaming (DStream)**, **Spark SQL**, và **DataFrame**. Demo tập trung vào việc đọc luồng dữ liệu giao dịch từ một socket, làm giàu dữ liệu bằng cách join với dữ liệu tĩnh từ file CSV, và lưu kết quả vào một thư mục đầu ra.

## Lý thuyết

### ETL trong bối cảnh thời gian thực
**ETL** (Extract - Transform - Load) là quy trình xử lý dữ liệu phổ biến trong phân tích dữ liệu:
- **Extract**: Trích xuất dữ liệu từ các nguồn như cơ sở dữ liệu, file, hoặc luồng dữ liệu.
- **Transform**: Biến đổi dữ liệu (làm sạch, làm giàu, hoặc tổng hợp) để đáp ứng yêu cầu nghiệp vụ.
- **Load**: Lưu dữ liệu đã xử lý vào đích đến (database, file, hoặc hệ thống báo cáo).

Trong bối cảnh **thời gian thực**, ETL yêu cầu xử lý dữ liệu ngay khi chúng được tạo ra, đảm bảo độ trễ thấp. **Spark Streaming** hỗ trợ xử lý luồng dữ liệu bằng cách chia dữ liệu thành các batch nhỏ (micro-batches), cho phép xử lý gần thời gian thực.

### Vai trò của Spark SQL và DataFrame
- **Spark SQL**: Cung cấp giao diện SQL để xử lý dữ liệu có cấu trúc, giúp viết các truy vấn phức tạp (như join, filter, aggregate) một cách dễ dàng và dễ đọc.
- **DataFrame**: Là cấu trúc dữ liệu cột trong Spark, tương tự như bảng trong cơ sở dữ liệu, hỗ trợ tích hợp với Spark SQL. DataFrame cho phép chuyển đổi DStream thành định dạng bảng, giúp thực hiện các phép biến đổi như join một cách hiệu quả.

Trong demo này, Spark SQL được sử dụng để join dữ liệu luồng (từ DStream) với dữ liệu tĩnh (từ file CSV), đơn giản hóa bước **Transform** trong quy trình ETL.

## Mô tả demo

### Tổng quan
Demo triển khai một pipeline Real-Time ETL với các bước:
1. **Extract**: Đọc luồng dữ liệu giao dịch (chứa `transaction_id`, `user_id`, `amount`) từ một socket (cổng 9999) sử dụng Spark Streaming DStream.
2. **Transform**: Join dữ liệu luồng với dữ liệu tĩnh (chứa `user_id`, `name`, `city`) từ file `users.csv` để làm giàu, bổ sung thông tin `user_name` và `user_city`.
3. **Load**: Lưu dữ liệu làm giàu vào thư mục `output/` dưới dạng file CSV.

### Công nghệ sử dụng
- **Apache Spark 3.4.0**: Xử lý luồng dữ liệu với DStream và thực hiện join với Spark SQL.
- **Python 3.9**: Viết script gửi dữ liệu luồng (`data_streamer.py`) và xử lý dữ liệu (`spark_app.py`).
- **Docker**: Triển khai môi trường với container Spark và Python.

### Nghiệp vụ
- **Nguồn dữ liệu luồng**: Dữ liệu giao dịch chỉ chứa `transaction_id`, `user_id`, và `amount`. Ví dụ: `txn_1234,1,150.75`.
- **Dữ liệu tĩnh**: File `users.csv` chứa thông tin người dùng với các trường `user_id`, `name`, và `city`. Ví dụ:
  ```csv
  user_id,name,city
  1,Alice,New York
  2,Bob,London
  3,Charlie,Paris
  ```
- **Mục tiêu**: Join dữ liệu luồng với dữ liệu tĩnh dựa trên `user_id` để tạo báo cáo chi tiết hơn, bao gồm `transaction_id`, `user_id`, `amount`, `user_name`, và `user_city`. Ví dụ:
  ```csv
  txn_1234,1,150.75,Alice,New York
  ```
- **Kết quả**: Dữ liệu làm giàu được lưu vào thư mục `output/` để sử dụng cho báo cáo hoặc phân tích.

## Cấu trúc dự án
```
spark-dstream-enrichment-demo/
├── docker-compose.yml      # Cấu hình Docker
├── spark_app.py           # Script Spark xử lý luồng và làm giàu dữ liệu
├── data_streamer.py       # Script gửi dữ liệu giả lập qua socket
├── users.csv              # File dữ liệu tĩnh
├── output/                # Thư mục lưu kết quả
└── README.md              # Tài liệu hướng dẫn
```

## Yêu cầu
- **Docker** và **Docker Compose** được cài đặt.
- **Cổng 9999** không bị chiếm dụng trên máy cục bộ.
- Máy có đủ bộ nhớ và CPU để chạy Spark và Python container.

## Hướng dẫn chạy demo

### 1. Chuẩn bị
- Clone repository hoặc tạo thư mục dự án với các file: `docker-compose.yml`, `spark_app.py`, `data_streamer.py`, `users.csv`, và thư mục `output/`.
- Đảm bảo các file được lưu trong thư mục dự án (ví dụ: `C:\Users\goku2\OneDrive\Desktop\demoenrich`).

### 2. Khởi động container
Chạy lệnh sau từ thư mục dự án:
```bash
docker-compose up -d
```

### 3. Cài đặt thư viện trong container `python-app-demo`
```bash
docker exec -it python-app-demo bash
pip install pyspark==3.4.0
exit
```

### 4. Chạy script gửi dữ liệu luồng
Mở terminal mới và chạy:
```bash
docker exec -it python-app-demo python /app/data_streamer.py
```
- Output:
<img width="1267" height="580" alt="Screenshot 2025-10-08 213511" src="https://github.com/user-attachments/assets/d4f26535-5340-439e-bbf1-20edd2219969" />

### 5. Chạy script Spark
Mở terminal mới và chạy:
```bash
docker exec -it spark-demo /opt/spark/bin/spark-submit --master local[*] /app/spark_app.py
```
- Output:

  Static data (users):
<img width="1443" height="391" alt="Screenshot 2025-10-08 213145" src="https://github.com/user-attachments/assets/9164a8a9-b48a-4ec4-bf69-a126764683ce" />

  Stream data (transactions):
<img width="1452" height="442" alt="Screenshot 2025-10-08 213419" src="https://github.com/user-attachments/assets/44983a12-e3ad-4d35-96e0-069d1ee68467" />

  Enriched data:
<img width="1447" height="380" alt="Screenshot 2025-10-08 213231" src="https://github.com/user-attachments/assets/773db594-e5ad-4bb5-bc17-b8c1f7ea628b" />


### 6. Kiểm tra kết quả
Kiểm tra thư mục `output/`:
```bash
cd output
ls
cat part-*
```
- Kỳ vọng:
  ```
  txn_1234,1,150.75,Alice,New York
  txn_5678,2,89.50,Bob,London
  ```

### 7. Dọn dẹp
```bash
docker-compose down -v
```

## Giải thích code

### Tổng quan
Script `spark_app.py` thực hiện pipeline Real-Time ETL:
1. **Extract**: Đọc dữ liệu luồng từ socket (cổng 9999) sử dụng DStream.
2. **Transform**: Chuyển DStream thành DataFrame, join với DataFrame tĩnh từ `users.csv` sử dụng Spark SQL.
3. **Load**: Lưu dữ liệu làm giàu vào thư mục `output/` dưới dạng CSV.

### Chi tiết code (`spark_app.py`)

1. **Khởi tạo SparkSession và StreamingContext**:
   ```python
   spark = SparkSession.builder.appName("DStreamEnrichmentDemo").getOrCreate()
   ssc = StreamingContext(spark.sparkContext, 5)
   ```
   - `SparkSession`: Được sử dụng để tạo DataFrame và chạy truy vấn Spark SQL.
   - `StreamingContext`: Được khởi tạo với batch interval 5 giây để xử lý luồng dữ liệu.

2. **Định nghĩa schema cho dữ liệu luồng**:
   ```python
   transaction_schema = StructType([
       StructField("transaction_id", StringType()),
       StructField("user_id", IntegerType()),
       StructField("amount", DoubleType())
   ])
   ```
   - Xác định cấu trúc của dữ liệu giao dịch để chuyển RDD thành DataFrame.

3. **Đọc dữ liệu tĩnh**:
   ```python
   static_df = spark.read.csv("/app/users.csv", header=True, inferSchema=True) \
       .select("user_id", "name", "city")
   static_df.createOrReplaceTempView("users")
   ```
   - Đọc file `users.csv` thành DataFrame `static_df`.
   - Đăng ký làm bảng tạm `users` để sử dụng trong Spark SQL.

4. **Đọc dữ liệu luồng từ socket**:
   ```python
   lines = ssc.socketTextStream("python-app-demo", 9999)
   ```
   - Tạo DStream từ socket trên container `python-app-demo`, cổng 9999.

5. **Xử lý DStream và join với Spark SQL**:
   ```python
   def process_rdd(rdd):
       if rdd.isEmpty():
           print("No new data received")
           return
       data = rdd.map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[1]), float(x[2])))
       stream_df = spark.createDataFrame(data, transaction_schema)
       stream_df.createOrReplaceTempView("transactions")
       enriched_df = spark.sql("""
           SELECT t.transaction_id, t.user_id, t.amount, u.name AS user_name, u.city AS user_city
           FROM transactions t
           LEFT JOIN users u ON t.user_id = u.user_id
       """)
       enriched_df.write.mode("append").csv("/app/output")
   lines.foreachRDD(process_rdd)
   ```
   - Chuyển mỗi RDD từ DStream thành DataFrame với schema `transaction_schema`.
   - Đăng ký DataFrame luồng làm bảng tạm `transactions`.
   - Sử dụng Spark SQL để thực hiện **LEFT JOIN** giữa `transactions` và `users` dựa trên `user_id`, tạo DataFrame `enriched_df` với các trường làm giàu (`user_name`, `user_city`).
   - Lưu kết quả vào thư mục `/app/output`.

6. **Khởi động stream**:
   ```python
   ssc.start()
   ssc.awaitTermination()
   ```
   - Bắt đầu xử lý luồng và giữ stream chạy cho đến khi bị dừng thủ công.

## Tài liệu tham khảo
- [Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Apache Spark GitHub Examples](https://github.com/apache/spark/tree/master/examples/src/main/python/streaming)
- [Docker Hub: apache/spark-py](https://hub.docker.com/r/apache/spark-py)
