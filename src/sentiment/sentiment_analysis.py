import sys
import os
import time
import argparse
import torch
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from typing import Iterator

def get_ready_datasets(processed_dir="data/processed"):

    if not os.path.exists(processed_dir):
        return []
    
    datasets = []
    for name in os.listdir(processed_dir):
        
        if name.endswith("_clean"):
            clean_name = name.replace("_clean", "")
            datasets.append(clean_name)
    return sorted(list(set(datasets)))

def main():
    
    parser = argparse.ArgumentParser(description="Run distributed sentiment analysis on Amazon reviews.")
    
    
    parser.add_argument("category", nargs="?", help="The category name (e.g. All_Beauty).")
    
    
    parser.add_argument(
        "--test", 
        action="store_true", 
        help="Run in TEST mode with only 1000 rows. Fast and safe."
    )
    args = parser.parse_args()

    
    if not args.category:
        print("\n" + "!"*50)
        print("[ERROR] You must provide a category name!")
        print("!"*50)
        
        ready_datasets = get_ready_datasets()
        
        if ready_datasets:
            print("\n[INFO] Found these CLEANED datasets ready for analysis:")
            for cat in ready_datasets:
                print(f"   - {cat}")
            print(f"\n[USAGE] Example: spark-submit src/sentiment/sentiment_analysis.py {ready_datasets[0]} --test")
        else:
            print("\n[INFO] No cleaned datasets found in 'data/processed/'.")
            print("       Please run the cleaning script (src/etl/clean_data.py) first!")
        
        sys.exit(1)

    category = args.category

    
    input_path = f"data/processed/{category}_clean"
    output_path = f"data/processed/{category}_sentiment"

    
    if not os.path.exists(input_path) and not os.path.exists(input_path + "/_SUCCESS"):
        print(f"\n[ERROR] Input path not found: {input_path}")
        print(f"        Have you run 'src/etl/clean_data.py {category}' yet?")
        sys.exit(1)

    
    limit_rows = None
    
    if args.test:
        print("\n" + "="*60)
        print(f"[TEST MODE ACTIVATED]: {category}")
        print("Processing ONLY 1000 rows.")
        print("="*60 + "\n")
        limit_rows = 1000
    else:
        
        print("\n" + "!"*60)
        print(f"[WARNING] FULL DATASET MODE ({category})")
        print("!"*60)
        print(f"Input:  {input_path}")
        print(f"Output: {output_path}")
        print("-" * 60)
        print("You are about to process the ENTIRE dataset.")
        print("This might take a long time.")
        print("-" * 60)
        print("Processing will start automatically in 10 seconds.")
        print("Press [Ctrl+C] NOW if you want to cancel!")
        print("-" * 60)
        
        try:
            for i in range(10, 0, -1):
                print(f"Starting in {i} seconds...", end="\r", flush=True)
                time.sleep(1)
            print("\n\nTime's up! Starting FULL processing...")
        except KeyboardInterrupt:
            print("\n\n[INFO] Operation cancelled by user.")
            os._exit(0)

    
    spark = SparkSession.builder \
        .appName(f"Sentiment_{category}_Analysis") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    print(f"[INFO] Reading data from {input_path}...")
    df = spark.read.parquet(input_path)
    
    
    try:
        df = df.select("asin", "review_time", "rating", "text", "year", "month", "user_id", "verified_purchase") \
               .filter(col("text").isNotNull() & (col("text") != ""))
    except Exception as e:
        print(f"[WARNING] Schema mismatch: {e}")
        
        df = df.select("rating", "text", "user_id", "year").filter(col("text").isNotNull())

    if limit_rows:
        df = df.limit(limit_rows)

    schema = StructType([
        StructField("sentiment_star", IntegerType(), True),
        StructField("sentiment_conf", FloatType(), True)
    ])

    @pandas_udf(schema)
    def predict_sentiment_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
        from transformers import pipeline
        
        device = 0 if torch.cuda.is_available() else -1
        model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
        classifier = pipeline("text-classification", model=model_name, device=device)

        for series in iterator:
            texts = series.to_list()
            preds = classifier(texts, truncation=True, batch_size=8)
            
            stars = [int(p['label'][0]) for p in preds]
            scores = [float(p['score']) for p in preds]
            
            yield pd.DataFrame({"sentiment_star": stars, "sentiment_conf": scores})

    print("[INFO] Starting distributed sentiment inference...")
    
    df_with_pred = df.withColumn("prediction", predict_sentiment_udf(col("text")))

    df_final = df_with_pred.select(
        col("*"),
        col("prediction.sentiment_star").alias("sentiment_star"),
        col("prediction.sentiment_conf").alias("sentiment_conf")
    ).drop("prediction")

    print(f"[INFO] Saving results to {output_path}...")
    df_final.write.mode("overwrite").parquet(output_path)
    print(f"[DONE] Output saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()