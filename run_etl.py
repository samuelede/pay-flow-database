"""
run_etl.py

One-click script to run the full ETL pipeline for Pay-Flow Database.

Order of execution:
1. Create raw tables (schema)
2. Load raw CSV data into raw_* tables
3. Run transformations and populate transformed/final tables

Usage:
    python run_etl.py
"""

from pathlib import Path
import subprocess
import sys


def run_script(script_path: Path, description: str):
    print("=" * 90)
    print(f"🚀 {description}")
    print("=" * 90)

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=False,
            text=True,
            check=True
        )
        print(f"✅ {description} completed successfully!\n")
        return True
    except subprocess.CalledProcessError:
        print(f"❌ {description} FAILED\n")
        return False
    except FileNotFoundError:
        print(f"❌ Script not found: {script_path}\n")
        return False


def main():
    print("🔄 PayFlow Database - Full ETL Pipeline\n")
    print("Note: Make sure the 'data/' folder contains the Olist CSV files.\n")

    root = Path(__file__).parent
    python_dir = root / "python"

    pipeline = [
        (python_dir / "load_raw_data.py",          "1. Load Raw Data (Schema + CSV Loading)"),
        (python_dir / "merge_into_transactions.py","2. Run Data Transformations"),
        (python_dir / "create_views.py", "3. Create Analytical Views"),
    ]

    success_count = 0

    for script_path, description in pipeline:
        if run_script(script_path, description):
            success_count += 1

    print("=" * 90)
    print("ETL PIPELINE SUMMARY")
    print("=" * 90)
    print(f"Steps completed successfully : {success_count}/3")

    if success_count == 3:
        print("🎉 FULL ETL PIPELINE COMPLETED SUCCESSFULLY!")
    else:
        print("⚠️  Some steps failed. Please check the errors above.")

    print("\n" + "=" * 90)


if __name__ == "__main__":
    main()