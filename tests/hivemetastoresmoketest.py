from telecom_project.spark_session import create_spark_session

spark = create_spark_session("hive-metastore-smoke-test")

spark.sql("DROP DATABASE IF EXISTS smoke_test_db CASCADE")
spark.sql("CREATE DATABASE IF NOT EXISTS smoke_test_db")
spark.sql("SHOW DATABASES").show()
spark.sql("USE smoke_test_db")
spark.sql("DROP TABLE IF EXISTS test_table")
spark.sql("CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING, age INT) USING PARQUET")
spark.sql("SHOW TABLES").show()
spark.sql("INSERT INTO test_table VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
spark.sql("SELECT * FROM test_table").show()

spark.stop()