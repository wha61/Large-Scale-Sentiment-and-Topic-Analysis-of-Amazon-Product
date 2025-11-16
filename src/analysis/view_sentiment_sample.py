from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ViewSentimentSample") \
        .getOrCreate()

    df = spark.read.parquet("data/processed/All_Beauty_sentiment_sample.parquet")

    print("Schema:")
    df.printSchema()

    print("\nFirst 20 rows:")
    df.show(20, truncate=80)

    print("\nRow count:", df.count())

    spark.stop()

if __name__ == "__main__":
    main()
