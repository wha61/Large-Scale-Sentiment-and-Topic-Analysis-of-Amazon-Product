from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transformers import pipeline
import torch


def label_to_star(label: str) -> int:
    """
    Hugging Face 'nlptown/bert-base-multilingual-uncased-sentiment' 返回的 label 形如:
      '1 star', '2 stars', ..., '5 stars'
    我们只取第一个字符并转成 int。
    """
    return int(label[0])


def main():
    spark = (
        SparkSession.builder
        .appName("SentimentAllBeautySpark")
        .getOrCreate()
    )

    input_path = "data/processed/All_Beauty_clean.parquet"
    output_path = "data/processed/All_Beauty_sentiment_sample.parquet"

    print(f"Reading cleaned data from: {input_path}")
    df = spark.read.parquet(input_path)

    df = df.select("rating", "text").where(
        (F.col("text").isNotNull()) & (F.col("text") != "")
    )

    total = df.count()
    print(f"Input row count: {total}")

    sample_n = 20000
    print(f"Sampling {sample_n} rows with random order...")
    df_sample = df.orderBy(F.rand()).limit(sample_n)

    sample_count = df_sample.count()
    print(f"Actual sample size: {sample_count}")

    pdf = df_sample.toPandas()

    #GPU（3070）
    device = 0 if torch.cuda.is_available() else -1
    print(f"Using device: {'GPU' if device == 0 else 'CPU'}")

    clf = pipeline(
        "text-classification",
        model="nlptown/bert-base-multilingual-uncased-sentiment",
        device=device
    )

    texts = pdf["text"].tolist()
    print("Running sentiment model on sample...")
    preds = clf(texts, batch_size=32, truncation=True)

    pdf["sentiment_star"] = [label_to_star(p["label"]) for p in preds]
    pdf["sentiment_conf"] = [p["score"] for p in preds]

    diff = (pdf["sentiment_star"] - pdf["rating"]).abs().mean()
    print("\nMean absolute difference between rating and sentiment_star:", float(diff))


    out_df = spark.createDataFrame(pdf)
    out_df.write.mode("overwrite").parquet(output_path)
    print(f"\nWrote sentiment sample to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
