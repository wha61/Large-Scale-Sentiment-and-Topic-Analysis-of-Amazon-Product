# src/analysis/macro_correlation.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt

def main():
    spark = SparkSession.builder.appName("MacroCorrelation").getOrCreate()

    df = spark.read.parquet("data/processed/all_beauty_sentiment")
    
    yearly_sentiment = df.groupBy("year").agg(
        F.avg("sentiment_star").alias("avg_sentiment"),
        F.avg("rating").alias("avg_rating"),
        F.count("*").alias("review_count")
    ).orderBy("year").toPandas()
    

    inflation_data = {
        2014: 1.6, 2015: 0.1, 2016: 1.3, 2017: 2.1, 
        2018: 2.4, 2019: 1.8, 2020: 1.2, 2021: 4.7, 
        2022: 8.0, 2023: 4.1
    }
    
    yearly_sentiment["inflation_rate"] = yearly_sentiment["year"].map(inflation_data)
    
    final_df = yearly_sentiment.dropna()
    
    print("=== Yearly Data ===")
    print(final_df)
    
    corr_sent = final_df["avg_sentiment"].corr(final_df["inflation_rate"])
    corr_rate = final_df["avg_rating"].corr(final_df["inflation_rate"])
    
    print(f"\nCorrelation (Sentiment vs Inflation): {corr_sent:.4f}")
    print(f"Correlation (Rating vs Inflation): {corr_rate:.4f}")
    
    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.set_xlabel('Year')
    ax1.set_ylabel('Average Sentiment', color='tab:blue')
    ax1.plot(final_df['year'], final_df['avg_sentiment'], color='tab:blue', marker='o', label='Sentiment')
    ax1.tick_params(axis='y', labelcolor='tab:blue')

    ax2 = ax1.twinx()
    ax2.set_ylabel('Inflation Rate (%)', color='tab:red')
    ax2.plot(final_df['year'], final_df['inflation_rate'], color='tab:red', marker='x', linestyle='--', label='Inflation')
    ax2.tick_params(axis='y', labelcolor='tab:red')

    plt.title(f'Sentiment vs Inflation over Years (Corr: {corr_sent:.2f})')
    plt.grid(True, alpha=0.3)
    plt.savefig("output/macro_correlation_plot.png")
    print("Saved plot to output/macro_correlation_plot.png")

    spark.stop()

if __name__ == "__main__":
    main()