"""Read booking events from Kafka, parse JSON, and print per-route aggregates (Structured Streaming)."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, sum
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Match producer topic and bootstrap; keep in sync with producer.py
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "booking-events"
APP_NAME = "SPAT-Streaming"


def booking_schema() -> StructType:
    """Schema for JSON payloads produced by producer.py."""
    return StructType(
        [
            StructField("route", StringType(), True),
            StructField("tickets", IntegerType(), True),
            StructField("price", IntegerType(), True),
        ]
    )


def main() -> None:
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    schema = booking_schema()

    # Raw Kafka stream: key/value/metadata columns
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .load()
    )

    # Deserialize message body as text, then parse JSON into typed columns
    json_strings = raw.selectExpr("CAST(value AS STRING) AS value")
    parsed = (
        json_strings.select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # Per-route totals: ticket count and revenue (tickets * price)
    aggregated = parsed.groupBy("route").agg(
        sum("tickets").alias("total_tickets"),
        sum(expr("tickets * price")).alias("total_revenue"),
    )

    # complete = full result table each trigger (fits unbounded groupBy on route)
    query = (
        aggregated.writeStream.outputMode("complete")
        .format("console")
        .start()
    )

    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
