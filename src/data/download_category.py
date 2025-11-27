# src/data/download_category_all_beauty.py
import argparse
from datasets import load_dataset

def download_category(category):
    """
    Downloads a specific category of Amazon Reviews 2023 dataset.
    """
    config = f"raw_review_{category}"
    print(f"Preparing to download category: {category}")
    print(f"   Config: {config}")

    try:
        ds = load_dataset(
            "McAuley-Lab/Amazon-Reviews-2023",
            config,
            split="full",
            trust_remote_code=True
        )
    except Exception as e:
        print(f"Error loading category '{category}': {e}")
        print("Please check the exact category name on readme")
        return

    out_path = f"data/raw/{category}_reviews"
    print(f"Saving to {out_path}...")
    ds.to_parquet(out_path)
    print(f"Done! Saved to {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Amazon Reviews 2023 dataset by category.")
    
    parser.add_argument(
        "category", 
        nargs="?",
        default="All_Beauty",
        help="The category name to download (e.g., All_Beauty, Electronics, Toys_and_Games, please check readme to get more info). Default is All_Beauty."
    )
    
    args = parser.parse_args()
    
    download_category(args.category)