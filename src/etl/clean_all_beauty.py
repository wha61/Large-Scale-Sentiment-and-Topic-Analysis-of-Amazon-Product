# src/etl/clean_all_beauty.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_timestamp,
    year,
    month,
    length,
    trim
)


def main():
    spark = SparkSession.builder \
        .appName("CleanAllBeautyReviews") \
        .getOrCreate()

    raw_path = "data/raw/all_beauty_reviews"
    out_path = "data/processed/all_beauty_clean"

    print(f"Reading raw data from: {raw_path}")
    df = spark.read.parquet(raw_path)

    # 1. Only filter useful data
    df = df.select(
        "rating",
        "title",
        "text",
        "asin",
        "parent_asin",
        "user_id",
        "timestamp",
        "verified_purchase",
        "helpful_vote"
    )

    # 2. filt text is null or all sace comment
    df = df.filter(
        (col("text").isNotNull()) &
        (length(trim(col("text"))) > 0)
    )

    # 3. Processing time: timestamp is in milliseconds, need to be /1000 to convert to seconds.
    df = df.withColumn(
        "review_time",
        to_timestamp(from_unixtime(col("timestamp") / 1000.0))
    )

    # 4. add year/month
    df = df.withColumn("year", year(col("review_time")))
    df = df.withColumn("month", month(col("review_time")))

    # 5. add text_length
    df = df.withColumn("text_length", length(col("text")))

    # 6. remove timestamp
    # df = df.drop("timestamp")

    print("\n=== Cleaned Schema ===")
    df.printSchema()

    print("\n=== Sample cleaned rows ===")
    df.show(5, truncate=80)

    print(f"\nTotal cleaned rows: {df.count()}")

    print(f"\nWriting cleaned data to: {out_path}")
    df.write.mode("overwrite").parquet(out_path)

    print("Done.")

    spark.stop()


if __name__ == "__main__":
    main()
