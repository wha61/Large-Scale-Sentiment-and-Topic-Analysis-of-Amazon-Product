# src/analysis/topic_modeling_mismatched.py
import pandas as pd
from bertopic import BERTopic
from pyspark.sql import SparkSession
import os

def main():
    spark = SparkSession.builder.appName("TopicModelingMismatched").getOrCreate()
    
    input_path = "data/processed/all_beauty_mismatched"
    
    if not os.path.exists(input_path):
        print(f"Error: Input path not found: {input_path}")
        print("Please check if the folder exists or run 'src/analysis/mismatched.py'.")
        spark.stop()
        return

    print(f"Loading data from {input_path}...")
    df = spark.read.parquet(input_path)
    
    pdf = df.select("text", "rating", "sentiment_star").limit(2000).toPandas()
    docs = pdf["text"].tolist()
    
    print(f"Loaded {len(docs)} documents.")

    if len(docs) < 5:
        print("Data too small (< 5 docs). BERTopic needs more data to run.")
        spark.stop()
        return

    print("Initializing BERTopic...")
    

    topic_model = BERTopic(language="multilingual", verbose=True, min_topic_size=2)
    
    print("Fitting topics...")
    try:
        topics, probs = topic_model.fit_transform(docs)
    except Exception as e:
        print(f"Error during fitting: {e}")
        spark.stop()
        return
    
    print("=== Topic Info ===")
    info = topic_model.get_topic_info()
    print(info.head(10))
    

    actual_topics = info[info['Topic'] != -1]

    if len(actual_topics) > 0:
        print(f"\nFound {len(actual_topics)} valid topics. Generating visualization...")
        try:
            n_topics = min(8, len(actual_topics))
            fig = topic_model.visualize_barchart(top_n_topics=n_topics)
            
            output_html = "output/topic_mismatched_barchart.html"
            fig.write_html(output_html)
            print(f"Saved visualization to {output_html}")
        except Exception as e:
            print(f"Could not generate visualization: {e}")
    else:
        print("\nNo stable topics found (all documents classified as outliers).")
        print("Skipping visualization to avoid crash.")


    output_csv = "output/mismatched_topics.csv"
    info.to_csv(output_csv, index=False)
    print(f"Saved topic info to {output_csv}")

    spark.stop()

if __name__ == "__main__":
    main()