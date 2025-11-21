# src/etl/inspect_amazon_parquet.py

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("InspectAmazonAllBeauty") \
        .getOrCreate()


    ##### SET HERE #####
    category = "all_beauty"
    ##### ######## #####


    path = f"data/raw/{category}_reviews"

    print(f"Reading {path} ...")
    df = spark.read.parquet(path)

    print("\n=== Schema ===")
    df.printSchema()

    print("\n=== Sample rows ===")
    df.select(
        "rating",
        "title",
        "text",
        "asin",
        "parent_asin",
        "user_id",
        "timestamp",
        "verified_purchase",
        "helpful_vote"
    ).show(5, truncate=80)

    print("\nTotal rows:", df.count())

    spark.stop()

if __name__ == "__main__":
    main()
