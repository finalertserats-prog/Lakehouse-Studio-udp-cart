"""
Enterprise Hadoop Bootstrap — Demo Hudi table
Creates hudi_demo.demo_orders as a Copy-on-Write Hudi table in HDFS,
partitioned by region.
Uses hudi-spark3.4-bundle (Spark 3.4.4 + Hudi 1.0.1).
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import uuid

spark = SparkSession.builder \
    .appName("Enterprise Hadoop Bootstrap") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.kryo.registrator",
            "org.apache.spark.HoodieSparkKryoRegistrar") \
    .getOrCreate()

spark.sql(
    "CREATE DATABASE IF NOT EXISTS hudi_demo "
    "LOCATION 'hdfs://namenode:9820/warehouse/hudi_demo.db'"
)

data = [
    (1, "ORD-001", "APAC", 1500.00),
    (2, "ORD-002", "EMEA", 2300.50),
    (3, "ORD-003", "AMER", 875.25),
    (4, "ORD-004", "APAC", 4200.00),
    (5, "ORD-005", "EMEA", 650.75),
]

df = spark.createDataFrame(data, ["order_id", "order_ref", "region", "amount"])
df = df.withColumn("ts", current_timestamp()).withColumn("uuid", lit(str(uuid.uuid4())))

df.write.format("hudi") \
    .option("hoodie.table.name", "demo_orders") \
    .option("hoodie.datasource.write.recordkey.field", "order_id") \
    .option("hoodie.datasource.write.partitionpath.field", "region") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.operation", "bulk_insert") \
    .option("hoodie.datasource.hive_sync.enable", "false") \
    .mode("overwrite") \
    .save("hdfs://namenode:9820/warehouse/hudi_demo.db/demo_orders")

print("Enterprise Hadoop demo lake created:")
print("  - hudi_demo.demo_orders (Hudi CoW, partitioned by region)")
spark.stop()
