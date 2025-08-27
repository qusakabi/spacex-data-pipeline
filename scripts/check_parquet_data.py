#!/usr/bin/env python3
"""
Script to validate data in Parquet files.
"""

import pandas as pd
import glob
import os
import logging
from etl.config.config import PARQUET_PATH


def check_parquet_data():
    """Validate Parquet data under PARQUET_PATH."""
    logging.info("=== Checking Parquet files ===")

    parquet_path = PARQUET_PATH

    if not os.path.exists(parquet_path):
        logging.error("Directory does not exist: %s", parquet_path)
        return False

    # Search for Parquet files
    parquet_files = glob.glob(f"{parquet_path}/*.parquet")

    if not parquet_files:
        logging.warning("No Parquet files found in %s", parquet_path)
        return False

    logging.info("Found %d Parquet files", len(parquet_files))

    total_rows = 0
    for file in parquet_files:
        logging.info("File: %s", os.path.basename(file))

        try:
            # Read Parquet file
            df = pd.read_parquet(file)

            logging.info("Shape: %d rows, %d columns", df.shape[0], df.shape[1])
            logging.info("Columns: %s", list(df.columns))

            if not df.empty:
                logging.info("First 3 records:")
                for i, (_, row) in enumerate(df.head(3).iterrows(), 1):
                    logging.info("   %d. %s", i, dict(row))

                # Data types
                logging.info("Dtypes:")
                for col, dtype in df.dtypes.items():
                    logging.info("   - %s: %s", col, dtype)

                total_rows += len(df)
            else:
                logging.warning("File is empty")

        except Exception as e:
            logging.error("Error reading file %s: %s", file, e, exc_info=True)

    logging.info("Total rows across files: %d", total_rows)

    return total_rows > 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    check_parquet_data()
