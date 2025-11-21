from pyspark.sql import SparkSession, functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("FindMismatchedReviewsAllBeauty")
        .getOrCreate()
    )

    input_path = "data/processed/all_beauty_sentiment"
    df = spark.read.parquet(input_path)

    needed_cols = ["rating", "text", "sentiment_star", "sentiment_conf"]
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in input: {missing}")

    df = df.withColumn(
        "diff",
        F.col("sentiment_star") - F.col("rating")
    ).withColumn(
        "abs_diff",
        F.abs(F.col("diff"))
    )

    THRESHOLD = 3.0

    mismatched = (
        df.filter(F.col("abs_diff") >= THRESHOLD)
          .orderBy(F.col("abs_diff").desc(), F.col("sentiment_conf").desc())
    )

    total_mismatch = mismatched.count()
    print(f"Found {total_mismatch} mismatched reviews with |rating - sentiment_star| >= {THRESHOLD}")

    mismatched.select(
        "rating",
        "sentiment_star",
        "sentiment_conf",
        "diff",
        "abs_diff",
        "text"
    ).show(20, truncate=False)

    top_k = 200
    top_mismatch = mismatched.limit(top_k)

    output_dir_csv = "output/mismatched_all_beauty_csv"
    output_dir_parquet = "data/processed/all_beauty_mismatched"

    (
        top_mismatch
        .select("rating", "sentiment_star", "sentiment_conf", "diff", "abs_diff", "text")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_dir_csv)
    )

    (
        top_mismatch
        .write
        .mode("overwrite")
        .parquet(output_dir_parquet)
    )

    print(f"Saved top {top_k} mismatched reviews to:")
    print(f"  CSV:     {output_dir_csv}")
    print(f"  Parquet: {output_dir_parquet}")

    spark.stop()

if __name__ == "__main__":
    main()
