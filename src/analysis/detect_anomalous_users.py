# src/analysis/detect_anomalous_users.py

import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_analyzed_datasets(processed_dir="data/processed"):
    if not os.path.exists(processed_dir):
        return []
    
    datasets = []
    for name in os.listdir(processed_dir):
        if name.endswith("_sentiment"):
            clean_name = name.replace("_sentiment", "")
            datasets.append(clean_name)
    return sorted(list(set(datasets)))

def main():
    parser = argparse.ArgumentParser(description="Detect anomalous users (spammers/conflicted) in Amazon reviews.")
    parser.add_argument("category", nargs="?", help="The category name (e.g. All_Beauty).")
    parser.add_argument("--cluster", action="store_true", help="Enable cluster mode (skips local path checks).")
    
    args = parser.parse_args()

    if not args.category:
        if args.cluster:
            print("[ERROR] In cluster mode, you MUST provide a category name.")
            sys.exit(1)

        print("\n" + "!"*50)
        print("[ERROR] You must provide a category name!")
        print("!"*50)
        
        ready_datasets = get_analyzed_datasets()
        if ready_datasets:
            print("\n[INFO] Found these datasets with SENTIMENT scores:")
            for cat in ready_datasets:
                print(f"   - {cat}")
            print(f"\n[USAGE] Example: spark-submit src/analysis/detect_anomalous_users.py {ready_datasets[0]}")
        else:
            print("\n[INFO] No sentiment datasets found in 'data/processed/'.")
            print("       Please run the sentiment analysis script first!")
        sys.exit(1)

    category = args.category
    input_path = f"data/processed/{category}_sentiment"
    output_path = f"output/{category}_suspicious_users"

    print(f"[INFO] Category:    {category}")
    print(f"[INFO] Input Path:  {input_path}")
    print(f"[INFO] Output Path: {output_path}")

    if args.cluster:
        print("[INFO] Cluster mode enabled: Skipping local path check.")
    else:
        if not os.path.exists(input_path) and not os.path.exists(input_path + "/_SUCCESS"):
            print(f"\n[ERROR] Input path not found: {input_path}")
            print(f"        Have you run 'src/sentiment/sentiment_analysis.py {category}' yet?")
            sys.exit(1)

    spark = SparkSession.builder \
        .appName(f"DetectAnomalousUsers_{category}") \
        .getOrCreate()

    print("[INFO] Reading data...")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"[ERROR] Failed to read parquet: {e}")
        spark.stop()
        sys.exit(1)

    print("[INFO] Aggregating user statistics...")
    user_stats = df.groupBy("user_id").agg(
        F.count("*").alias("review_count"),
        F.avg("rating").alias("avg_rating"),
        F.stddev("rating").alias("rating_stddev"),
        F.avg("sentiment_star").alias("avg_sentiment"),
        F.countDistinct("asin").alias("product_count")
    )
    
    suspicious_spammers = user_stats.filter(
        (F.col("review_count") >= 5) & 
        (F.col("rating_stddev").isNull() | (F.col("rating_stddev") < 0.1))
    ).withColumn("reason", F.lit("Potential Spammer (High Vol, No Variance)"))

    suspicious_conflicted = user_stats.filter(
        (F.col("review_count") >= 3) &
        (F.col("avg_rating") > 4.5) &
        (F.col("avg_sentiment") < 2.5)
    ).withColumn("reason", F.lit("Conflicted (High Rating, Low Sentiment)"))

    print("\n=== Suspicious Spammers (Sample) ===")
    suspicious_spammers.orderBy(F.col("review_count").desc()).show(10)

    print("\n=== Conflicted Users (Sample) ===")
    suspicious_conflicted.orderBy(F.col("review_count").desc()).show(10)

    print(f"[INFO] Saving report to {output_path}...")
    final_report = suspicious_spammers.unionByName(suspicious_conflicted)

    final_report.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    print("[DONE] Analysis complete.")
    spark.stop()

if __name__ == "__main__":
    main()