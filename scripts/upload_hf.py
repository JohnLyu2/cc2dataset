from datasets import load_dataset, DatasetDict, Dataset
from huggingface_hub import HfApi
from pathlib import Path

# Define dataset and directory
dataset_repo = "JohnLyu/cc_main_2025_05_first10wat_pdf_links"  # Change to your HF repo
parquet_dir = "/Users/zhengyanglu/Desktop/Numina/cc2dataset/outputs/2025-02-18-00-33-43/"  # Directory with Parquet files

# Get all parquet file 
parquet_dir = Path(parquet_dir)
parquet_files = [str(f) for f in parquet_dir.glob("*.parquet")]

if not parquet_files:
    raise ValueError(f"No Parquet files found in {parquet_dir}")

# Load all Parquet files into a single dataset
dataset = load_dataset("parquet", data_files=parquet_files)

# Push dataset to Hugging Face
dataset.push_to_hub(dataset_repo)

print(f"Dataset successfully uploaded to: https://huggingface.co/datasets/{dataset_repo}")