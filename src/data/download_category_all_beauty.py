from datasets import load_dataset

def download_category(category="All_Beauty"):
    config = f"raw_review_{category}"
    print(f"Loading config: {config}")

    ds = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        config,
        split="full",
        trust_remote_code=True
    )

    out_path = f"data/raw/{category}_reviews"
    print(f"Saving to {out_path}")
    ds.to_parquet(out_path)
    print("Done!")

if __name__ == "__main__":
    download_category("all_beauty")
