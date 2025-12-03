import sys
import os
import argparse
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

def get_available_datasets(raw_dir="data/raw"):
    if not os.path.exists(raw_dir):
        return []
    
    datasets = []
    
    for name in os.listdir(raw_dir):
        
        if "_reviews" in name:
            
            clean_name = name.split("_reviews")[0]
            datasets.append(clean_name)
    return sorted(list(set(datasets)))

def main(category):
    spark = SparkSession.builder \
        .appName(f"Clean_{category}_Reviews") \
        .getOrCreate()

    
    
    
    # base_raw = f"data/raw/{category}_reviews"
    # if os.path.exists(base_raw):
    #     raw_path = base_raw
    # else:
    #     raw_path = base_raw + ".parquet"
    
    # out_path = f"data/processed/{category}_clean"
    raw_path = f"data/raw/{category}_reviews"
    
    out_path = f"data/processed/{category}_clean"

    print(f"\nStarting ETL Job")
    print(f"   Category: {category}")
    print(f"   Input:    {raw_path}")
    print(f"   Output:   {out_path}")

    
    try:
        df = spark.read.parquet(raw_path)
    except Exception as e:
        print(f"\nError reading path: {raw_path}")
        print("Please check if the category name is correct.")
        spark.stop()
        return

    

    
    
    try:
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
    except Exception:
        
        print("Warning: Standard columns not found, selecting subset.")
        df = df.select("rating", "text", "user_id", "timestamp", "asin")

    
    df = df.filter(
        (col("text").isNotNull()) &
        (length(trim(col("text"))) > 0)
    )

    
    df = df.withColumn(
        "review_time",
        to_timestamp(from_unixtime(col("timestamp") / 1000.0))
    )

    
    df = df.withColumn("year", year(col("review_time")))
    df = df.withColumn("month", month(col("review_time")))

    
    df = df.withColumn("text_length", length(col("text")))

    
    

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
    parser = argparse.ArgumentParser(description="Clean Amazon Reviews Data")
    
    
    parser.add_argument("category", nargs="?", help="The category name to process.")
    
    args = parser.parse_args()

    
    if not args.category:
        print("\n" + "!"*50)
        print("ERROR: You must provide a category name!")
        print("!"*50)
        
        
        available = get_available_datasets()
        
        if available:
            print("\nFound these datasets in 'data/raw/':")
            for cat in available:
                print(f"   - {cat}")
            print(f"\nUsage Example: spark-submit src/etl/clean_data.py {available[0]}")
        else:
            print("\nNo datasets found in 'data/raw/'. Please run the downloader first!")
        
        sys.exit(1)
    
    
    main(args.category)