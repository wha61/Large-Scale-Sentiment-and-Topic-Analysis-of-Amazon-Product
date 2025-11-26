import sys
import os
import argparse
from pyspark.sql import SparkSession, functions as F

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
    
    parser = argparse.ArgumentParser(description="Calculate consistency stats between User Rating and AI Sentiment.")
    parser.add_argument("category", nargs="?", help="The category name (e.g. All_Beauty).")
    args = parser.parse_args()

    
    if not args.category:
        print("\n" + "!"*50)
        print("[ERROR] You must provide a category name!")
        print("!"*50)
        
        ready_datasets = get_analyzed_datasets()
        
        if ready_datasets:
            print("\n[INFO] Found these datasets with SENTIMENT scores:")
            for cat in ready_datasets:
                print(f"   - {cat}")
            print(f"\n[USAGE] Example: spark-submit src/analysis/rating_vs_sentiment.py {ready_datasets[0]}")
        else:
            print("\n[INFO] No sentiment datasets found in 'data/processed/'.")
            print("       Please run the sentiment analysis script first!")
        
        sys.exit(1)

    category = args.category

    
    input_path = f"data/processed/{category}_sentiment"
    output_path = f"output/{category}_rating_vs_sentiment"

    print(f"[INFO] Category:    {category}")
    print(f"[INFO] Input Path:  {input_path}")
    print(f"[INFO] Output Path: {output_path}")

    
    if not os.path.exists(input_path) and not os.path.exists(input_path + "/_SUCCESS"):
        print(f"\n[ERROR] Input path not found: {input_path}")
        print(f"        Have you run 'src/sentiment/sentiment_analysis.py {category}' yet?")
        sys.exit(1)

    
    spark = (
        SparkSession.builder
        .appName(f"RatingVsSentiment_{category}")
        .getOrCreate()
    )

    print("[INFO] Reading data...")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"[ERROR] Failed to read parquet: {e}")
        spark.stop()
        sys.exit(1)

    
    needed_cols = ["rating", "sentiment_star", "sentiment_conf"]
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        print(f"[ERROR] Missing columns in input: {missing}")
        spark.stop()
        sys.exit(1)

    df = df.select("rating", "sentiment_star", "sentiment_conf")

    
    print("[INFO] Calculating global statistics...")
    overall_diff = df.select(
        F.abs(F.col("rating") - F.col("sentiment_star")).alias("diff")
    ).agg(F.avg("diff")).first()[0]
    
    print(f"\n[RESULT] Overall Mean Absolute Error (MAE): {overall_diff:.4f}")

    
    print("[INFO] Aggregating per-rating statistics...")
    summary = (
        df.groupBy("rating")
          .agg(
              F.avg("sentiment_star").alias("avg_sentiment_star"),
              F.avg(F.abs(F.col("rating") - F.col("sentiment_star"))).alias("avg_abs_diff"),
              F.count("*").alias("count")
          )
          .orderBy("rating")
    )

    print("\n=== Per-Rating Summary ===")
    summary.show(truncate=False)

    
    print(f"[INFO] Saving summary CSV to {output_path}...")
    (
        summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"[DONE] File saved successfully.")

    spark.stop()

if __name__ == "__main__":
    main()