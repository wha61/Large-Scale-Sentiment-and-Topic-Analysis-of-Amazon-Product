# src/analysis/detect_anomalous_users.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("DetectAnomalousUsers").getOrCreate()

    df = spark.read.parquet("data/processed/all_beauty_sentiment")

    user_stats = df.groupBy("user_id").agg(
        F.count("*").alias("review_count"),
        F.avg("rating").alias("avg_rating"),
        F.stddev("rating").alias("rating_stddev"),
        F.avg("sentiment_star").alias("avg_sentiment"),
        F.countDistinct("asin").alias("product_count")
    )
    
    suspicious_spammers = user_stats.filter(
        (F.col("review_count") >= 10) & 
        (F.col("rating_stddev").isNull() | (F.col("rating_stddev") < 0.1))
    ).withColumn("reason", F.lit("Potential Spammer (High Vol, No Variance)"))

    suspicious_conflicted = user_stats.filter(
        (F.col("review_count") >= 5) &
        (F.col("avg_rating") > 4.5) &
        (F.col("avg_sentiment") < 2.5)
    ).withColumn("reason", F.lit("Conflicted (High Rating, Low Sentiment)"))

    print("=== Suspicious Spammers (Sample) ===")
    suspicious_spammers.orderBy(F.col("review_count").desc()).show(10)

    print("=== Conflicted Users (Sample) ===")
    suspicious_conflicted.orderBy(F.col("review_count").desc()).show(10)

    suspicious_spammers.unionByName(suspicious_conflicted) \
        .write.mode("overwrite").csv("output/suspicious_users_analysis", header=True)

    spark.stop()

if __name__ == "__main__":
    main()