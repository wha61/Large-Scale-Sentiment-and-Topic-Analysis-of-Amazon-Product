from pyspark.sql import SparkSession, functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("RatingVsSentimentAllBeauty")
        .getOrCreate()
    )

    path = "data/processed/All_Beauty_sentiment_sample.parquet"
    df = spark.read.parquet(path)

    df = df.select("rating", "sentiment_star", "sentiment_conf")

    overall_diff = df.select(
        F.abs(F.col("rating") - F.col("sentiment_star")).alias("diff")
    ).agg(F.avg("diff")).first()[0]
    print(f"Overall mean |rating - sentiment_star| = {overall_diff:.4f}")

    summary = (
        df.groupBy("rating")
          .agg(
              F.avg("sentiment_star").alias("avg_sentiment_star"),
              F.avg(F.abs(F.col("rating") - F.col("sentiment_star"))).alias("avg_abs_diff"),
              F.count("*").alias("count")
          )
          .orderBy("rating")
    )

    print("\nPer-rating summary:")
    summary.show(truncate=False)

    output_path = "output/rating_vs_sentiment_all_beauty_sample"
    (
        summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"\nSaved summary to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
