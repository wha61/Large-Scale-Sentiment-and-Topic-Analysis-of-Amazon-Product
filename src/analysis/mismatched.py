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
    
    parser = argparse.ArgumentParser(description="Find mismatched reviews (Rating vs Sentiment) for a category.")
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
            print(f"\n[USAGE] Example: spark-submit src/analysis/mismatched.py {ready_datasets[0]}")
        else:
            print("\n[INFO] No sentiment datasets found in 'data/processed/'.")
            print("       Please run the sentiment analysis script first!")
        
        sys.exit(1)

    category = args.category

    
    input_path = f"data/processed/{category}_sentiment"
    
    
    output_dir_csv = f"output/{category}_mismatched_csv"
    
    output_dir_parquet = f"data/processed/{category}_mismatched"

    print(f"[INFO] Category:        {category}")
    print(f"[INFO] Input Path:      {input_path}")
    print(f"[INFO] Output CSV:      {output_dir_csv}")
    print(f"[INFO] Output Parquet:  {output_dir_parquet}")

    
    if not os.path.exists(input_path) and not os.path.exists(input_path + "/_SUCCESS"):
        print(f"\n[ERROR] Input path not found: {input_path}")
        print(f"        Have you run 'src/sentiment/sentiment_analysis.py {category}' yet?")
        sys.exit(1)

    
    spark = (
        SparkSession.builder
        .appName(f"FindMismatchedReviews_{category}")
        .getOrCreate()
    )

    print("[INFO] Reading data...")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"[ERROR] Failed to read parquet: {e}")
        spark.stop()
        sys.exit(1)

    needed_cols = ["rating", "text", "sentiment_star", "sentiment_conf"]
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        print(f"[ERROR] Missing columns in input: {missing}")
        spark.stop()
        sys.exit(1)

    print("[INFO] Calculating differences...")
    df = df.withColumn(
        "diff",
        F.col("sentiment_star") - F.col("rating")
    ).withColumn(
        "abs_diff",
        F.abs(F.col("diff"))
    )

    THRESHOLD = 3.0

    mismatched = (
        df.filter(F.col("abs_diff") >= THRESHOLD)
          .orderBy(F.col("abs_diff").desc(), F.col("sentiment_conf").desc())
    )

    total_mismatch = mismatched.count()
    print(f"[RESULT] Found {total_mismatch} mismatched reviews with |rating - sentiment_star| >= {THRESHOLD}")

    print("\n=== Sample Mismatched Reviews ===")
    mismatched.select(
        "rating",
        "sentiment_star",
        "sentiment_conf",
        "diff",
        "abs_diff",
        "text"
    ).show(10, truncate=False)

    
    top_k = 5000  
    top_mismatch = mismatched.limit(top_k)

    print(f"[INFO] Saving top {top_k} mismatched reviews...")

    
    (
        top_mismatch
        .select("rating", "sentiment_star", "sentiment_conf", "diff", "abs_diff", "text")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_dir_csv)
    )

    
    (
        top_mismatch
        .write
        .mode("overwrite")
        .parquet(output_dir_parquet)
    )

    print(f"[DONE] Data saved successfully.")
    print(f"       CSV:     {output_dir_csv}")
    print(f"       Parquet: {output_dir_parquet}")

    spark.stop()

if __name__ == "__main__":
    main()