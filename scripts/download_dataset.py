#!/usr/bin/env python3
"""
Dataset Downloader & Preparer for Fake News Classification Pipeline
Downloads 'emineyetm/fake-news-detection-datasets' using kagglehub
and sets up raw data directory for model training.
"""

import os
import shutil
import sys
from pathlib import Path

def download_and_setup_dataset(target_dir: str = "data/raw") -> str:
    print("=" * 60)
    print("Fake News Classification Pipeline - Dataset Downloader")
    print("=" * 60)

    target_path = Path(target_dir)
    target_path.mkdir(parents=True, exist_ok=True)

    true_csv = target_path / "True.csv"
    fake_csv = target_path / "Fake.csv"

    if true_csv.exists() and fake_csv.exists():
        print(f"Dataset already exists in '{target_dir}'.")
        print(f" - True.csv: {true_csv.stat().st_size / (1024*1024):.2f} MB")
        print(f" - Fake.csv: {fake_csv.stat().st_size / (1024*1024):.2f} MB")
        return str(target_path)

    try:
        import kagglehub
        print("Downloading latest version of 'emineyetm/fake-news-detection-datasets' via kagglehub...")
        download_path = kagglehub.dataset_download("emineyetm/fake-news-detection-datasets")
        print("Path to dataset files from kagglehub:", download_path)

        downloaded_dir = Path(download_path)
        # Search for True.csv and Fake.csv inside downloaded directory
        found_true = list(downloaded_dir.rglob("True.csv"))
        found_fake = list(downloaded_dir.rglob("Fake.csv"))

        if found_true and found_fake:
            print(f"Copying files from {download_path} to {target_dir}...")
            shutil.copy(found_true[0], true_csv)
            shutil.copy(found_fake[0], fake_csv)
            print("Successfully copied True.csv and Fake.csv!")
            return str(target_path)
        else:
            print("Warning: CSV files not found directly under downloaded directory.")
    except Exception as e:
        print(f"Notice: kagglehub download encountered: {e}")

    # Fallback to sample data if full raw download is unavailable
    sample_dir = Path("data/sample")
    sample_true = sample_dir / "True_sample.csv"
    sample_fake = sample_dir / "Fake_sample.csv"

    if sample_true.exists() and sample_fake.exists():
        print("Using bundled sample dataset (True_sample.csv, Fake_sample.csv)...")
        shutil.copy(sample_true, true_csv)
        shutil.copy(sample_fake, fake_csv)
        print("Fallback sample dataset copied successfully.")
        return str(target_path)

    print("Error: Could not obtain dataset files.")
    return str(target_path)

if __name__ == "__main__":
    download_and_setup_dataset()
