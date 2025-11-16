# src/etl/inspect_all_beauty_with_sentiment.py

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("InspectAllBeautyWithSentiment") \
        .getOrCreate()

    path = "data/processed/All_Beauty_with_sentiment.parquet"
    df = spark.read.parquet(path)

    df.select(
        "rating", "sentiment_star", "sentiment_conf",
        "year", "month", "text_length"
    ).show(10, truncate=80)

    mae = df.selectExpr("avg(abs(sentiment_star - rating)) as mae").collect()[0]["mae"]
    print("\nGlobal MAE between rating and sentiment_star:", mae)

    spark.stop()

if __name__ == "__main__":
    main()
