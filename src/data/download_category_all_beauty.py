"""
Download Amazon Product Reviews Dataset from Hugging Face

Downloads raw review data for a specified category and saves as Parquet.

Usage:
    python src/data/download_category_all_beauty.py

Args:
    category (str): Product category name (e.g., "All_Beauty", "Toys_and_Games")
                   Must match exact case. Default: "All_Beauty"

split (str): Data split to download. Options:
             - "full": Download all data (no train/test split)
             - "train": Training set (if available)
             - "test": Test set (if available)
             - "train[:1000]": First 1000 samples from train set
             Default: "full"
"""

from datasets import load_dataset
import os

def download_category(category="All_Beauty"):
    config = f"raw_review_{category}"
    print(f"Loading category: {config}")

    ds = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        config,
        split="full",
        # trust_remote_code=True
    )

    out_dir = f"data/raw"
    os.makedirs(out_dir, exist_ok=True)

    out_path = f"data/raw/{category}_reviews.parquet"
    print(f"Saving to {out_path}")
    ds.to_parquet(out_path)
    print("Done!")

if __name__ == "__main__":
    download_category("All_Beauty")
