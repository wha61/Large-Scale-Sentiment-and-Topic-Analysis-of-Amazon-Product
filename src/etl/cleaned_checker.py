import sys
import os
import argparse
from pyspark.sql import SparkSession

def get_cleaned_datasets(processed_dir="data/processed"):
    if not os.path.exists(processed_dir):
        return []
    
    datasets = []
    for name in os.listdir(processed_dir):
        if name.endswith("_clean"):
            clean_name = name.replace("_clean", "")
            datasets.append(clean_name)
    return sorted(list(set(datasets)))

def main():
    
    parser = argparse.ArgumentParser(description="Inspect cleaned Amazon Reviews data.")
    parser.add_argument("category", nargs="?", help="The category name (e.g. All_Beauty).")
    args = parser.parse_args()

    
    if not args.category:
        print("\n" + "!"*50)
        print("[ERROR] You must provide a category name!")
        print("!"*50)
        
        ready_datasets = get_cleaned_datasets()
        
        if ready_datasets:
            print("\n[INFO] Found these CLEANED datasets:")
            for cat in ready_datasets:
                print(f"   - {cat}")
            print(f"\n[USAGE] Example: spark-submit src/etl/inspect_cleaned_data.py {ready_datasets[0]}")
        else:
            print("\n[INFO] No cleaned datasets found in 'data/processed/'.")
            print("       Please run 'src/etl/clean_data.py' first!")
        
        sys.exit(1)

    category = args.category

    
    path = f"data/processed/{category}_clean"
    print(f"[INFO] Inspecting path: {path}")

    if not os.path.exists(path) and not os.path.exists(path + "/_SUCCESS"):
        print(f"\n[ERROR] Path not found: {path}")
        print(f"        Have you run 'src/etl/clean_data.py {category}' yet?")
        sys.exit(1)

    
    spark = SparkSession.builder \
        .appName(f"InspectCleaned_{category}") \
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
            "rating", "year", "month",
            "verified_purchase", "helpful_vote",
            "text_length", "title", "text"
        ).show(5, truncate=80)
    except Exception:
        print("[WARNING] Standard columns not found, showing raw data...")
        df.show(5, truncate=80)

    count = df.count()
    print(f"\n[RESULT] Total cleaned rows: {count}")

    spark.stop()

if __name__ == "__main__":
    main()