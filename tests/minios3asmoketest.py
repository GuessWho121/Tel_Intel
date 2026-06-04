from telecom_project.spark_session import create_spark_session

spark = create_spark_session("minio-spark-smoke-test")


df1 = spark.createDataFrame([
    (1, "Alice", 29),
    (2, "Bob", 31),
    (3, "Charlie", 25),
    (4, "David", 35),
    (5, "Eve", 28)
], ["id", "name", "age"]
)

df1.show(5)

df1.write.\
    format("parquet").\
    mode("overwrite").\
    save("s3a://airtel-spark/smoke/s3asmoketest.parquet")

df2 = spark.read.\
    format("parquet").\
    load("s3a://airtel-spark/smoke/s3asmoketest.parquet")

df2.show(5)
print("row count: ", df2.count())

spark.stop()