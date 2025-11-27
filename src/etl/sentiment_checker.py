import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, abs, avg

def get_sentiment_datasets(processed_dir="data/processed"):

    if not os.path.exists(processed_dir):
        return []
    
    datasets = []
    for name in os.listdir(processed_dir):
        if name.endswith("_sentiment"):
            clean_name = name.replace("_sentiment", "")
            datasets.append(clean_name)
    return sorted(list(set(datasets)))

def main():
    
    parser = argparse.ArgumentParser(description="Inspect sentiment analysis results (Parquet).")
    parser.add_argument("category", nargs="?", help="The category name (e.g. All_Beauty).")
    args = parser.parse_args()

    
    if not args.category:
        print("\n" + "!"*50)
        print("[ERROR] You must provide a category name!")
        print("!"*50)
        
        ready_datasets = get_sentiment_datasets()
        
        if ready_datasets:
            print("\n[INFO] Found these datasets with SENTIMENT scores:")
            for cat in ready_datasets:
                print(f"   - {cat}")
            print(f"\n[USAGE] Example: spark-submit src/etl/inspect_sentiment_data.py {ready_datasets[0]}")
        else:
            print("\n[INFO] No sentiment datasets found in 'data/processed/'.")
            print("       Please run the sentiment analysis script first!")
        
        sys.exit(1)

    category = args.category

    
    path = f"data/processed/{category}_sentiment"
    print(f"[INFO] Inspecting path: {path}")

    if not os.path.exists(path) and not os.path.exists(path + "/_SUCCESS"):
        print(f"\n[ERROR] Path not found: {path}")
        print(f"        Have you run 'src/sentiment/sentiment_analysis.py {category}' yet?")
        sys.exit(1)

    
    spark = SparkSession.builder \
        .appName(f"InspectSentiment_{category}") \
        .getOrCreate()

    print("[INFO] Reading parquet...")
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"[ERROR] Failed to read data: {e}")
        spark.stop()
        sys.exit(1)

    
    if "text_length" not in df.columns:
        print("[INFO] Note: 'text_length' column missing. Calculating on the fly...")
        df = df.withColumn("text_length", length(col("text")))

    print("\n=== Sample Data (First 10 rows) ===")
    try:
        df.select(
            "rating", "sentiment_star", "sentiment_conf",
            "year", "month", "text_length"
        ).show(10, truncate=80)
    except Exception:
        print("[WARNING] Standard columns not found, showing raw data...")
        df.show(10, truncate=80)

    print("[INFO] Calculating Global MAE...")
    try:
        mae = df.select(
            avg(abs(col("sentiment_star") - col("rating"))).alias("mae")
        ).collect()[0]["mae"]
        print(f"\n[RESULT] Global MAE between rating and sentiment_star: {mae:.4f}")
    except Exception as e:
        print(f"[ERROR] Could not calculate MAE: {e}")

    spark.stop()

if __name__ == "__main__":
    main()