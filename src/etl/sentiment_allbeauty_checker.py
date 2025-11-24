# src/etl/inspect_all_beauty_with_sentiment.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, abs, avg

def main():
    spark = SparkSession.builder \
        .appName("InspectAllBeautyWithSentiment") \
        .getOrCreate()

    path = "data/processed/all_beauty_sentiment"
    print(f"Reading from {path}...")
    df = spark.read.parquet(path)

    if "text_length" not in df.columns:
        print("Note: 'text_length' column missing. Calculating on the fly...")
        df = df.withColumn("text_length", length(col("text")))

    print("\n=== Sample Data (First 10 rows) ===")
    df.select(
        "rating", "sentiment_star", "sentiment_conf",
        "year", "month", "text_length"
    ).show(10, truncate=80)

    mae = df.select(
        avg(abs(col("sentiment_star") - col("rating"))).alias("mae")
    ).collect()[0]["mae"]
    
    print(f"\nGlobal MAE between rating and sentiment_star: {mae:.4f}")

    spark.stop()

if __name__ == "__main__":
    main()