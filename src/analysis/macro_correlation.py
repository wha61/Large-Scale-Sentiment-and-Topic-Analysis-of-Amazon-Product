import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt

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
    
    parser = argparse.ArgumentParser(description="Analyze macro-economic correlation (Inflation vs Sentiment).")
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
            print(f"\n[USAGE] Example: python src/analysis/macro_correlation.py {ready_datasets[0]}")
        else:
            print("\n[INFO] No sentiment datasets found in 'data/processed/'.")
            print("       Please run the sentiment analysis script first!")
        
        sys.exit(1)

    category = args.category

    
    input_path = f"data/processed/{category}_sentiment"
    
    output_plot_path = f"output/{category}_macro_correlation_plot.png"

    print(f"[INFO] Category:    {category}")
    print(f"[INFO] Input Path:  {input_path}")
    print(f"[INFO] Output Plot: {output_plot_path}")

    if not os.path.exists(input_path) and not os.path.exists(input_path + "/_SUCCESS"):
        print(f"\n[ERROR] Input path not found: {input_path}")
        print(f"        Have you run 'src/sentiment/sentiment_analysis.py {category}' yet?")
        sys.exit(1)

    
    spark = SparkSession.builder.appName(f"MacroCorrelation_{category}").getOrCreate()

    print("[INFO] Reading data...")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"[ERROR] Failed to read parquet: {e}")
        spark.stop()
        sys.exit(1)
    
    print("[INFO] Aggregating yearly statistics...")
    yearly_sentiment = df.groupBy("year").agg(
        F.avg("sentiment_star").alias("avg_sentiment"),
        F.avg("rating").alias("avg_rating"),
        F.count("*").alias("review_count")
    ).orderBy("year").toPandas()
    
    
    
    inflation_data = {
        1996: 3.0, 1997: 2.3, 1998: 1.6, 1999: 2.2,
        2000: 3.4, 2001: 2.8, 2002: 1.6, 2003: 2.3,
        2004: 2.7, 2005: 3.4, 2006: 3.2, 2007: 2.8,
        2008: 3.8, 2009: -0.4, 2010: 1.6, 2011: 3.2,
        2012: 2.1, 2013: 1.5, 2014: 1.6, 2015: 0.1, 
        2016: 1.3, 2017: 2.1, 2018: 2.4, 2019: 1.8, 
        2020: 1.2, 2021: 4.7, 2022: 8.0, 2023: 4.1
    }
    
    
    yearly_sentiment["inflation_rate"] = yearly_sentiment["year"].map(inflation_data)
    
    
    final_df = yearly_sentiment.dropna()
    
    if final_df.empty:
        print("[ERROR] No overlapping data between reviews and inflation data.")
        spark.stop()
        return

    print("\n=== Yearly Data Summary ===")
    print(final_df)
    
    
    if len(final_df) < 3:
        print("\n[WARNING] Not enough years of data to calculate meaningful correlation.")
    else:
        corr_sent = final_df["avg_sentiment"].corr(final_df["inflation_rate"])
        corr_rate = final_df["avg_rating"].corr(final_df["inflation_rate"])
        
        print(f"\n[RESULT] Correlation (Sentiment vs Inflation): {corr_sent:.4f}")
        print(f"[RESULT] Correlation (Rating vs Inflation):    {corr_rate:.4f}")
        
        
        print(f"[INFO] Generating plot...")
        fig, ax1 = plt.subplots(figsize=(10, 6))

        ax1.set_xlabel('Year')
        ax1.set_ylabel('Average Sentiment', color='tab:blue')
        ax1.plot(final_df['year'], final_df['avg_sentiment'], color='tab:blue', marker='o', label='Sentiment')
        ax1.tick_params(axis='y', labelcolor='tab:blue')

        ax2 = ax1.twinx()
        ax2.set_ylabel('Inflation Rate (%)', color='tab:red')
        ax2.plot(final_df['year'], final_df['inflation_rate'], color='tab:red', marker='x', linestyle='--', label='Inflation')
        ax2.tick_params(axis='y', labelcolor='tab:red')

        plt.title(f'[{category}] Sentiment vs Inflation (Corr: {corr_sent:.2f})')
        plt.grid(True, alpha=0.3)
        plt.savefig(output_plot_path)
        print(f"[DONE] Saved plot to {output_plot_path}")

    spark.stop()

if __name__ == "__main__":
    main()