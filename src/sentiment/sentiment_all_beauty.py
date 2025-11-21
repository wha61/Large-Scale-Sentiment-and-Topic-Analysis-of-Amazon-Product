# src/sentiment/sentiment_all_beauty_spark.py

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

def main():
    parser = argparse.ArgumentParser(description="Run distributed sentiment analysis on Amazon reviews.")
    parser.add_argument(
        "--test", 
        action="store_true", 
        help="Run in TEST mode with only 1000 rows. Fast and safe."
    )
    args = parser.parse_args()

    limit_rows = None
    
    if args.test:
        print("\n" + "="*60)
        print("TEST MODE ACTIVATED")
        print("Processing ONLY 1000 rows.")
        print("="*60 + "\n")
        limit_rows = 1000
    else:
        # 全量模式警告
        print("\n" + "!"*60)
        print("WARNING: FULL DATASET MODE")
        print("!"*60)
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
            print("\n\nOperation cancelled by user.")
            os._exit(0)

    spark = SparkSession.builder \
        .appName("SentimentAllBeautySpark_Scalable") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    input_path = "data/processed/all_beauty_clean"
    output_path = "data/processed/all_beauty_sentiment"

    print(f"Reading data from {input_path}...")
    df = spark.read.parquet(input_path)
    
    df = df.select("asin", "review_time", "rating", "text", "year", "month", "user_id", "verified_purchase") \
           .filter(col("text").isNotNull() & (col("text") != ""))

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

    print("Starting distributed sentiment inference...")
    
    df_with_pred = df.withColumn("prediction", predict_sentiment_udf(col("text")))

    df_final = df_with_pred.select(
        col("*"),
        col("prediction.sentiment_star").alias("sentiment_star"),
        col("prediction.sentiment_conf").alias("sentiment_conf")
    ).drop("prediction")

    print("Saving results...")
    df_final.write.mode("overwrite").parquet(output_path)
    print(f"Done. Output saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()