import sys
import os
import argparse
import pandas as pd
from bertopic import BERTopic
from pyspark.sql import SparkSession

def get_mismatched_datasets(processed_dir="data/processed"):
    if not os.path.exists(processed_dir):
        return []
    
    datasets = []
    for name in os.listdir(processed_dir):
        
        if name.endswith("_mismatched"):
            clean_name = name.replace("_mismatched", "")
            datasets.append(clean_name)
    return sorted(list(set(datasets)))

def main():
    
    parser = argparse.ArgumentParser(description="Run BERTopic modeling on mismatched reviews.")
    parser.add_argument("category", nargs="?", help="The category name (e.g. All_Beauty).")
    args = parser.parse_args()

    
    if not args.category:
        print("\n" + "!"*50)
        print("[ERROR] You must provide a category name!")
        print("!"*50)
        
        ready_datasets = get_mismatched_datasets()
        
        if ready_datasets:
            print("\n[INFO] Found these MISMATCHED datasets ready for topic modeling:")
            for cat in ready_datasets:
                print(f"   - {cat}")
            print(f"\n[USAGE] Example: python src/analysis/topic_modeling_mismatched.py {ready_datasets[0]}")
        else:
            print("\n[INFO] No mismatched datasets found in 'data/processed/'.")
            print("       Please run 'src/analysis/mismatched.py' first!")
        
        sys.exit(1)

    category = args.category

    
    
    input_path = f"data/processed/{category}_mismatched"
    
    
    output_html = f"output/{category}_topic_mismatched_barchart.html"
    output_csv = f"output/{category}_mismatched_topics.csv"

    print(f"[INFO] Category:    {category}")
    print(f"[INFO] Input Path:  {input_path}")
    print(f"[INFO] Output HTML: {output_html}")
    print(f"[INFO] Output CSV:  {output_csv}")

    if not os.path.exists(input_path) and not os.path.exists(input_path + "/_SUCCESS"):
        print(f"\n[ERROR] Input path not found: {input_path}")
        print(f"        Have you run 'src/analysis/mismatched.py {category}' yet?")
        sys.exit(1)

    
    spark = SparkSession.builder.appName(f"TopicModeling_{category}").getOrCreate()

    print("[INFO] Loading data...")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"[ERROR] Failed to read parquet: {e}")
        spark.stop()
        sys.exit(1)
    
    
    
    pdf = df.select("text", "rating", "sentiment_star").limit(5000).toPandas()
    docs = pdf["text"].tolist()
    
    print(f"[INFO] Loaded {len(docs)} documents.")

    
    if len(docs) < 5:
        print("[ERROR] Data too small (< 5 docs). BERTopic needs more data to run.")
        print("        Try running the pipeline without --test to get more mismatched reviews.")
        spark.stop()
        sys.exit(1)

    
    print("[INFO] Initializing BERTopic...")
    
    topic_model = BERTopic(language="multilingual", verbose=True, min_topic_size=2)
    
    print("[INFO] Fitting topics...")
    try:
        topics, probs = topic_model.fit_transform(docs)
    except Exception as e:
        print(f"[ERROR] Error during fitting: {e}")
        spark.stop()
        sys.exit(1)
    
    print("\n=== Topic Info ===")
    info = topic_model.get_topic_info()
    print(info.head(10))
    
    
    
    actual_topics = info[info['Topic'] != -1]

    if len(actual_topics) > 0:
        print(f"\n[INFO] Found {len(actual_topics)} valid topics. Generating visualization...")
        try:
            
            n_topics = min(8, len(actual_topics))
            fig = topic_model.visualize_barchart(top_n_topics=n_topics)
            
            fig.write_html(output_html)
            print(f"[DONE] Saved visualization to {output_html}")
        except Exception as e:
            print(f"[WARNING] Could not generate visualization: {e}")
    else:
        print("\n[WARNING] No stable topics found (all documents classified as outliers).")
        print("          Skipping visualization.")

    
    info.to_csv(output_csv, index=False)
    print(f"[DONE] Saved topic info to {output_csv}")

    spark.stop()

if __name__ == "__main__":
    main()