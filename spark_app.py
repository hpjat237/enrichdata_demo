from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

def main():
    # Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("DStreamEnrichmentDemo") \
        .getOrCreate()

    # Khởi tạo StreamingContext với batch interval 5 giây
    ssc = StreamingContext(spark.sparkContext, 5)

    # Định nghĩa schema cho dữ liệu giao dịch
    transaction_schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("amount", DoubleType())
    ])

    # Đọc dữ liệu tĩnh từ users.csv
    static_df = spark.read.csv("/app/users.csv", header=True, inferSchema=True) \
        .select("user_id", "name", "city")
    static_df.createOrReplaceTempView("users")
    print("Static data (users):")
    static_df.show()

    # Đọc dữ liệu luồng từ socket
    lines = ssc.socketTextStream("python-app-demo", 9999)
    # Hàm xử lý mỗi RDD
    def process_rdd(rdd):
        if rdd.isEmpty():
            print("No new data received")
            return

        try:
            # Chuyển RDD thành DataFrame
            data = rdd.map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[1]), float(x[2])))
            stream_df = spark.createDataFrame(data, transaction_schema)
            stream_df.createOrReplaceTempView("transactions")
            print("Stream data (transactions):")
            stream_df.show(truncate=False)

            # Thực hiện join với Spark SQL
            enriched_df = spark.sql("""
                SELECT t.transaction_id, t.user_id, t.amount, u.name AS user_name, u.city AS user_city
                FROM transactions t
                LEFT JOIN users u ON t.user_id = u.user_id
            """)
            print("Enriched data:")
            enriched_df.show(truncate=False)

            # Lưu kết quả vào thư mục output
            enriched_df.write \
                .mode("append") \
                .csv("/app/output")
            print(f"Saved enriched data to /app/output")
        except Exception as e:
            print(f"Error processing RDD: {str(e)}")

    # Áp dụng xử lý cho DStream
    lines.foreachRDD(process_rdd)

    # Bắt đầu stream
    ssc.start()
    print("Started streaming...")
    try:
        ssc.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping stream...")
        ssc.stop(stopSparkContext=True, stopGraceFully=True)
        spark.stop()

if __name__ == "__main__":
    main()