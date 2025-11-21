from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("InspectCleanedAllBeauty") \
        .getOrCreate()

    path = "data/processed/all_beauty_clean"
    df = spark.read.parquet(path)

    print("=== Schema ===")
    df.printSchema()

    print("\n=== Sample rows ===")
    df.select(
        "rating", "year", "month",
        "verified_purchase", "helpful_vote",
        "text_length", "title", "text"
    ).show(5, truncate=80)

    print("\nTotal cleaned rows:", df.count())

    spark.stop()

if __name__ == "__main__":
    main()
