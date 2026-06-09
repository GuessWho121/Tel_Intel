import os
from pyspark.sql import SparkSession
from telecom_project.config import load_cluster_config

def create_spark_session(app_name: str) -> SparkSession:
    config = load_cluster_config()

    spark_cfg = config["spark"]
    minio_cfg = config["minio"]
    iceberg_cfg = config["iceberg"]
    hive_cfg = config["hive"]

    access_key = os.getenv(minio_cfg["access_key_env"])
    secret_key = os.getenv(minio_cfg["secret_key_env"])

    if not access_key or not secret_key:
        raise ValueError("MinIO access key and secret key are missing from environment variables.")

    sparkbuild = SparkSession.builder.\
        appName(app_name).\
        master(spark_cfg["master_url"]).\
        config("spark.hadoop.fs.s3a.endpoint", minio_cfg["endpoint"]).\
        config("spark.hadoop.fs.s3a.access.key", access_key).\
        config("spark.hadoop.fs.s3a.secret.key", secret_key).\
        config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg["path_style_access"]).lower()).\
        config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio_cfg["ssl_enabled"]).lower()).\
        config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").\
        config("spark.sql.adaptive.enabled", "true").\
        config("spark.sql.warehouse.dir", hive_cfg["warehouse_location"]).\
        config("spark.hadoop.hive.metastore.uris", hive_cfg["metastore_uri"]).\
        config("hive.metastore.uris", hive_cfg["metastore_uri"]).\
        config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider").\
        config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1").\
        config("spark.hadoop.fs.s3a.change.detection.mode", "none").\
        enableHiveSupport()
        # config("spark.sql.catalog.telecom.uri", hive_cfg["metastore_uri"]).\
    
    return sparkbuild.getOrCreate()
        