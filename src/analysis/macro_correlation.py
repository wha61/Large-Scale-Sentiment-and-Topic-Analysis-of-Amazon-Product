

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

def load_inflation_from_csv(csv_path):
    print(f"[INFO] Loading inflation data from: {csv_path}")
    
    if not os.path.exists(csv_path):
        print(f"[ERROR] Inflation CSV file not found at: {csv_path}")
        return None

    try:
        
        df = pd.read_csv(csv_path, skiprows=4)
        
        
        usa_data = df[df['Country Code'] == 'USA']
        
        if usa_data.empty:
            print("[ERROR] 'USA' country code not found in inflation CSV.")
            return None
        
        
        inflation_dict = {}
        
        for col in usa_data.columns:
            if col.isdigit(): 
                try:
                    year = int(col)
                    val = usa_data.iloc[0][col]
                    
                    if pd.notnull(val):
                        inflation_dict[year] = float(val)
                except ValueError:
                    continue 
        
        if not inflation_dict:
            print("[ERROR] No valid yearly inflation data extracted for USA.")
            return None

        print(f"[INFO] Successfully loaded inflation data for {len(inflation_dict)} years.")
        return inflation_dict

    except Exception as e:
        print(f"[ERROR] Failed to parse inflation CSV: {e}")
        return None

def main():
    
    parser = argparse.ArgumentParser(description="Analyze macro-economic correlation (Inflation vs Sentiment).")
    parser.add_argument("category", nargs="?", help="The category name (e.g. All_Beauty).")
    
    
    parser.add_argument("--inflation_file", default="data/external/API_FP.CPI.TOTL.ZG_DS2_en_csv_v2_280663.csv", 
                        help="Path to World Bank Inflation CSV file.")
    
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

    
    inflation_data = load_inflation_from_csv(args.inflation_file)
    
    if not inflation_data:
        print("\n" + "!"*60)
        print("[CRITICAL ERROR] Could not load inflation data.")
        print(f"File checked: {args.inflation_file}")
        print("Please ensure the CSV file exists and contains valid USA data.")
        print("Script aborted.")
        print("!"*60)
        sys.exit(1)

    
    spark = SparkSession.builder.appName(f"MacroCorrelation_{category}").getOrCreate()

    print("[INFO] Reading sentiment data...")
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
    
    
    yearly_sentiment["inflation_rate"] = yearly_sentiment["year"].map(inflation_data)
    
    
    final_df = yearly_sentiment.dropna()
    
    if final_df.empty:
        print("[ERROR] No overlapping data between reviews and inflation data.")
        print(f"Review Years: {sorted(yearly_sentiment['year'].unique())}")
        print(f"Inflation Years (Loaded): {sorted(list(inflation_data.keys()))}")
        spark.stop()
        sys.exit(1)

    print("\n=== Yearly Data Summary ===")
    print(final_df)
    
    
    if len(final_df) < 3:
        print("\n[WARNING] Not enough years of data to calculate meaningful correlation (Need at least 3 years).")
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
        plt.tight_layout()
        plt.savefig(output_plot_path)
        print(f"[DONE] Saved plot to {output_plot_path}")

    spark.stop()

if __name__ == "__main__":
    main()