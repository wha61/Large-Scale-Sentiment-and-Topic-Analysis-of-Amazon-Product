import sys
import os
import argparse
from pyspark.sql import SparkSession

def get_raw_datasets(raw_dir="data/raw"):
    if not os.path.exists(raw_dir):
        return []
    
    datasets = []
    for name in os.listdir(raw_dir):
        
        if "_reviews" in name:
            clean_name = name.split("_reviews")[0]
            datasets.append(clean_name)
    return sorted(list(set(datasets)))

def main():
    
    parser = argparse.ArgumentParser(description="Inspect RAW Amazon Reviews data (Parquet).")
    parser.add_argument("category", nargs="?", help="The category name (e.g. All_Beauty).")
    args = parser.parse_args()

    
    if not args.category:
        print("\n" + "!"*50)
        print("[ERROR] You must provide a category name!")
        print("!"*50)
        
        ready_datasets = get_raw_datasets()
        
        if ready_datasets:
            print("\n[INFO] Found these RAW datasets in 'data/raw/':")
            for cat in ready_datasets:
                print(f"   - {cat}")
            print(f"\n[USAGE] Example: spark-submit src/etl/inspect_raw_data.py {ready_datasets[0]}")
        else:
            print("\n[INFO] No raw datasets found in 'data/raw/'.")
            print("       Please run the downloader script (src/data/download_category_all_beauty.py) first!")
        
        sys.exit(1)

    category = args.category

    
    
    base_path = f"data/raw/{category}_reviews"
    
    if os.path.exists(base_path):
        path = base_path
    elif os.path.exists(base_path + ".parquet"):
        path = base_path + ".parquet"
    else:
        
        path = base_path

    print(f"[INFO] Inspecting path: {path}")

    if not os.path.exists(path) and not os.path.exists(path + "/_SUCCESS"):
        print(f"\n[ERROR] Path not found: {path}")
        print(f"        Have you run the downloader for '{category}' yet?")
        sys.exit(1)

    
    spark = SparkSession.builder \
        .appName(f"InspectRaw_{category}") \
        .getOrCreate()

    print("[INFO] Reading parquet...")
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"[ERROR] Failed to read data: {e}")
        spark.stop()
        sys.exit(1)

    print("\n=== Schema ===")
    df.printSchema()

    print("\n=== Sample Rows (First 5) ===")
    
    try:
        df.select(
            "rating",
            "title",
            "text",
            "asin",
            "user_id",
            "timestamp",
            "verified_purchase"
        ).show(5, truncate=80)
    except Exception:
        print("[WARNING] Standard columns not found, showing raw data...")
        df.show(5, truncate=80)

    count = df.count()
    print(f"\n[RESULT] Total raw rows: {count}")

    spark.stop()

if __name__ == "__main__":
    main()