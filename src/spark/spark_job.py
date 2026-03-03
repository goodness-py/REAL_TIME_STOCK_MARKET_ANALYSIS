import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema must match exactly what the producer sends
schema = StructType([
    StructField("id",     StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("date",   StringType(), True),
    StructField("open",   FloatType(),  True),
    StructField("high",   FloatType(),  True),
    StructField("low",    FloatType(),  True),
    StructField("close",  FloatType(),  True),
])

MYSQL_URL = "jdbc:mysql://db:3306/stock_db"
MYSQL_PROPS = {
    "user":     "root",
    "password": "Anya@3683",
    "driver":   "com.mysql.cj.jdbc.Driver"
}


def create_spark_session():
    return SparkSession.builder \
        .appName("StockMarketAnalysis") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()


def process_batch(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")

    if batch_df.count() == 0:
        logger.info("Empty batch, skipping...")
        return

    # Parse JSON from Kafka value
    parsed_df = batch_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Drop rows where symbol is null (failed to parse)
    parsed_df = parsed_df.filter(col("symbol").isNotNull())

    logger.info(f"Parsed {parsed_df.count()} valid records")

    # Write raw records to stocks table
    parsed_df.write.jdbc(
        url=MYSQL_URL,
        table="stocks",
        mode="append",
        properties=MYSQL_PROPS
    )

    # Calculate analytics per symbol
    analytics_df = parsed_df.groupBy("symbol").agg(
        round(avg("open"),  2).alias("avg_open"),
        round(avg("high"),  2).alias("avg_high"),
        round(avg("low"),   2).alias("avg_low"),
        round(avg("close"), 2).alias("avg_close"),
    )

    # Write analytics to stock_analytics table
    analytics_df.write.jdbc(
        url=MYSQL_URL,
        table="stock_analytics",
        mode="overwrite",
        properties=MYSQL_PROPS
    )

    logger.info(f"Batch {batch_id} written to MySQL!")


def main():
    logger.info("Starting Spark Job...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "stock_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    # Write using foreachBatch
    query = raw_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    logger.info("Spark streaming started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()