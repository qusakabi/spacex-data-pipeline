#!/usr/bin/env python3
"""
Script to validate data in the spacex_launches table in Snowflake.
"""

import logging
from etl.utils.snowflake_utils import SnowflakeManager
from etl.config.config import SNOWFLAKE_SCHEMA


def check_snowflake_data():
    """Validate data in Snowflake."""
    logger = logging.getLogger(__name__)
    logger.info("=== Checking data in Snowflake ===")

    # Create Snowflake manager
    snowflake_manager = SnowflakeManager()

    if not snowflake_manager.connect():
        logger.error("Failed to connect to Snowflake")
        return False

    try:
        # Resolve table name with optional schema
        table_base = "spacex_launches"
        table_name = f"{SNOWFLAKE_SCHEMA}.{table_base}" if SNOWFLAKE_SCHEMA else table_base

        # Check if table exists
        if snowflake_manager.check_table_exists(table_name):
            logger.info("Table %s exists", table_name)

            # Get table info
            table_info = snowflake_manager.get_table_info(table_name)
            if table_info:
                logger.info("Table structure:")
                for col in table_info["columns"]:
                    logger.info("   - %s: %s", col["name"], col["type"])

            # Count rows
            count_result = snowflake_manager.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            if count_result:
                row_count = count_result[0][0]
                logger.info("Row count: %d", row_count)

                if row_count > 0:
                    # Show first 5 records
                    logger.info("First 5 records:")
                    sample_result = snowflake_manager.execute_query(f"SELECT * FROM {table_name} LIMIT 5")
                    if sample_result:
                        for i, row in enumerate(sample_result, 1):
                            logger.info("   %d. %s", i, row)
                else:
                    logger.warning("Table is empty - no data")
            else:
                logger.error("Failed to fetch row count")
        else:
            logger.error("Table %s does not exist", table_name)

    except Exception as e:
        logger.error("Error validating data: %s", e, exc_info=True)
        return False

    finally:
        snowflake_manager.disconnect()

    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    check_snowflake_data()
