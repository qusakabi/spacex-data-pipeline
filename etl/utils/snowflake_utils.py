import snowflake.connector
import pandas as pd
import logging
from typing import Optional, List, Dict, Any
from etl.config.settings import SNOWFLAKE

logger = logging.getLogger(__name__)

class SnowflakeManager:
    """Manager for Snowflake connection and operations."""
    
    def __init__(self):
        self.conn = None
        self.settings = SNOWFLAKE
    
    def connect(self):
        """Establish a connection to Snowflake."""
        try:
            self.conn = snowflake.connector.connect(**self.settings.connector_kwargs())
            logger.info(f"Connected to Snowflake: {self.settings.account}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}", exc_info=True)
            return False
    
    def disconnect(self):
        """Close the Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Snowflake")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[tuple]]:
        """Execute a SQL query."""
        if not self.conn:
            if not self.connect():
                return None
        
        try:
            cur = self.conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                result = cur.fetchall()
            else:
                result = None
                self.conn.commit()
            
            cur.close()
            return result
        except Exception as e:
            logger.error(f"Error executing query: {e}", exc_info=True)
            return None
    
    def create_table_if_not_exists(self, table_name: str, columns: List[Dict[str, str]]):
        """Create a table if it does not exist."""
        column_definitions = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            if col.get('primary_key'):
                col_def += " PRIMARY KEY"
            if col.get('not_null'):
                col_def += " NOT NULL"
            column_definitions.append(col_def)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_definitions)}
        )
        """
        
        return self.execute_query(create_table_sql)
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """Insert a DataFrame into a Snowflake table."""
        if df.empty:
            logger.warning("DataFrame is empty, nothing to insert")
            return False
        
        if if_exists == 'replace':
            self.execute_query(f"TRUNCATE TABLE {table_name}")
        
        # Prepare data for insertion
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append(tuple(row))
        
        # Get column names
        columns = list(df.columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
        """
        
        # Insert data
        try:
            cur = self.conn.cursor()
            cur.executemany(insert_sql, data_to_insert)
            self.conn.commit()
            cur.close()
            logger.info(f"Successfully inserted {len(data_to_insert)} rows into {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error inserting data: {e}", exc_info=True)
            return False
    
    def insert_dataframe_copy_into(self, df: pd.DataFrame, table_name: str):
        """Insert a DataFrame into a Snowflake table via a temporary table."""
        if df.empty:
            logger.warning("DataFrame is empty, nothing to insert")
            return False
        
        try:
            # Create a temporary table for the data
            temp_table_name = f"temp_{table_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Build temporary table DDL
            columns = list(df.columns)
            column_definitions = []
            for col in columns:
                if col == 'date_utc':
                    column_definitions.append(f"{col} TIMESTAMP_NTZ")
                elif col == 'success':
                    column_definitions.append(f"{col} BOOLEAN")
                else:
                    column_definitions.append(f"{col} STRING")
            
            create_temp_sql = f"""
            CREATE TEMPORARY TABLE {temp_table_name} (
                {', '.join(column_definitions)}
            )
            """
            
            cur = self.conn.cursor()
            cur.execute(create_temp_sql)
            
            # Insert rows into the temporary table
            for _, row in df.iterrows():
                values = []
                for col in columns:
                    if col == 'date_utc':
                        # Convert string back to timestamp
                        values.append(f"'{row[col]}'")
                    elif col == 'success':
                        values.append(str(row[col]).lower())
                    else:
                        # Escape single quotes
                        safe_value = str(row[col]).replace("'", "''")
                        values.append(f"'{safe_value}'")
                
                insert_sql = f"""
                INSERT INTO {temp_table_name} ({', '.join(columns)})
                VALUES ({', '.join(values)})
                """
                cur.execute(insert_sql)
            
            # Copy data from temporary to target table
            copy_sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table_name}
            """
            
            cur.execute(copy_sql)
            
            # Drop the temporary table
            cur.execute(f"DROP TABLE {temp_table_name}")
            
            self.conn.commit()
            cur.close()
            
            logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting data: {e}", exc_info=True)
            return False
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a table."""
        query = f"DESCRIBE TABLE {table_name}"
        result = self.execute_query(query)
        
        if result:
            columns = []
            for row in result:
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'null': row[2],
                    'default': row[3],
                    'primary_key': row[4],
                    'unique_key': row[5]
                })
            
            return {
                'table_name': table_name,
                'columns': columns
            }
        return None
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists (supports schema.table)."""
        schema = None
        name = table_name
        if "." in table_name:
            parts = table_name.split(".", 1)
            schema, name = parts[0].strip('"'), parts[1].strip('"')

        if schema:
            query = f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{name.upper()}'
            """
        else:
            query = f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{name.upper()}'
            """
        result = self.execute_query(query)
        return bool(result and result[0][0] > 0)

def get_snowflake_connection():
    """Utility function to get a connected Snowflake manager."""
    manager = SnowflakeManager()
    if manager.connect():
        return manager
    return None
