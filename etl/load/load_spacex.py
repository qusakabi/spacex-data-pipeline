import os
import glob
import logging
import pandas as pd
from etl.utils.snowflake_utils import SnowflakeManager
from etl.config.config import SNOWFLAKE_SCHEMA

logger = logging.getLogger(__name__)

def load_parquet_to_snowflake(path: str):
    """Load data from Parquet files into Snowflake"""
    
    # Create Snowflake manager
    snowflake_manager = SnowflakeManager()
    
    if not snowflake_manager.connect():
        logger.error("Failed to connect to Snowflake")
        return False
    
    try:
        # Define table structure
        table_columns = [
            {'name': 'id', 'type': 'STRING', 'primary_key': True, 'not_null': True},
            {'name': 'name', 'type': 'STRING', 'not_null': True},
            {'name': 'date_utc', 'type': 'TIMESTAMP_NTZ', 'not_null': True},
            {'name': 'success', 'type': 'BOOLEAN', 'not_null': True},
            {'name': 'rocket', 'type': 'STRING', 'not_null': True}
        ]
        
        # Resolve table name with optional schema
        table_base = 'spacex_launches'
        table_name = f"{SNOWFLAKE_SCHEMA}.{table_base}" if SNOWFLAKE_SCHEMA else table_base

        # Create table if not exists
        snowflake_manager.create_table_if_not_exists(table_name, table_columns)
        
        # Read Parquet files
        parquet_files = glob.glob(f"{path}/*.parquet")
        if not parquet_files:
            logger.warning("No parquet files found in %s", path)
            return False
        
        total_rows = 0
        for file in parquet_files:
            logger.info("Processing file: %s", file)
            df = pd.read_parquet(file)
            
            if not df.empty:
                # Preprocess data before insert
                df['id'] = df['id'].astype(str)
                df['name'] = df['name'].astype(str)
                df['rocket'] = df['rocket'].astype(str)
                df['success'] = df['success'].astype(bool)
                
                # Convert timestamp to string for safe insertion
                df['date_utc'] = pd.to_datetime(df['date_utc']).dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Insert using a temporary table for efficiency
                if snowflake_manager.insert_dataframe_copy_into(df, table_name):
                    total_rows += len(df)
                    logger.info("Successfully loaded %d rows from %s", len(df), file)
                else:
                    logger.error("Failed to load data from %s", file)
        
        logger.info("Total rows loaded: %d", total_rows)
        return True
        
    except Exception as e:
        logger.error("Error during data loading: %s", e, exc_info=True)
        return False
    
    finally:
        snowflake_manager.disconnect()

# For debugging
if __name__ == "__main__":
    logger.info("Snowflake Configuration:")
    logger.info("Account: %s", os.environ.get("SNOWFLAKE_ACCOUNT", "Not set"))
    logger.info("User: %s", os.environ.get("SNOWFLAKE_USER", "Not set"))
    logger.info("Database: %s", os.environ.get("SNOWFLAKE_DATABASE", "Not set"))
    logger.info("Schema: %s", os.environ.get("SNOWFLAKE_SCHEMA", "Not set"))
    logger.info("Warehouse: %s", os.environ.get("SNOWFLAKE_WAREHOUSE", "Not set"))