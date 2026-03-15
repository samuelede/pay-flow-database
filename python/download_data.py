from pathlib import Path
import urllib.request
import zipfile
import pandas as pd
import sys


def download_olist_dataset(force_download: bool = False):
    print("📥 Starting download of Brazilian E-Commerce (Olist) dataset...\n")

    data_dir = Path("/dataset")
    data_dir.mkdir(exist_ok=True)

    zip_path = data_dir / "brazilian_ecommerce.zip"

    # List of critical files we expect after extraction
    expected_files = [
        "olist_customers_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_payments_dataset.csv",
    ]

    # === Check if data already exists ===
    existing_files = [f for f in expected_files if (data_dir / f).exists()]

    if existing_files and not force_download:
        print(f"✅ Found {len(existing_files)}/{len(expected_files)} expected CSV files in '{data_dir}/'")
        
        response = input("\n📂 Dataset already exists. Do you want to re-download it? (y/N): ").strip().lower()
        
        if response not in ['y', 'yes']:
            print("⏭️  Skipping download. Using existing files.")
            verify_files(data_dir)
            return
        else:
            print("🔄 Re-downloading dataset as requested...\n")

    # === Download the dataset ===
    url = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"

    print("Downloading dataset (this may take 30-90 seconds)...")
    try:
        urllib.request.urlretrieve(url, zip_path)
        print("✅ Download completed!\n")
    except Exception as e:
        print(f"❌ Download failed: {e}")
        print("\n💡 Tip: The direct URL often requires authentication.")
        print("   Recommended: Use the official Kaggle API instead (see previous version).")
        return

    # === Extract the dataset ===
    print("📦 Extracting files...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)
        print("✅ Extraction completed!\n")
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return

    # === Verify files ===
    verify_files(data_dir)


def verify_files(data_dir: Path):
    """Verify the downloaded files and show summary."""
    print("🔍 Verifying key files...\n")
    
    try:
        customers = pd.read_csv(data_dir / "olist_customers_dataset.csv")
        sellers   = pd.read_csv(data_dir / "olist_sellers_dataset.csv")
        items     = pd.read_csv(data_dir / "olist_order_items_dataset.csv")

        print(f"✅ Customers     : {len(customers):,} rows")
        print(f"✅ Sellers       : {len(sellers):,} rows")
        print(f"✅ Order Items   : {len(items):,} rows")
        print("\n🎉 All data downloaded and verified successfully!")
        
    except FileNotFoundError as e:
        print(f"❌ Some files are missing: {e}")
        print("Please check the 'data/' folder manually.")


if __name__ == "__main__":
    # You can force re-download by running: python download_data.py --force
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', 
                       help='Force re-download even if files already exist')
    args = parser.parse_args()

    download_olist_dataset(force_download=args.force)