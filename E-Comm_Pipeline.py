import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
from sqlalchemy import create_engine
import yaml
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ecommerce_etl')

class EcommerceETL:
    def __init__(self, config_path: str = 'config.yml'):
        """Initialize ETL pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.engine = create_engine(self.config['warehouse']['connection_string'])
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def extract_sales_data(self, start_date: datetime) -> pd.DataFrame:
        """Extract sales data from source system."""
        try:
            # Simulate loading from a database/CSV
            sales_data = pd.read_csv(self.config['source_paths']['sales'])
            sales_data['date'] = pd.to_datetime(sales_data['date'])
            sales_data = sales_data[sales_data['date'] >= start_date]
            logger.info(f"Extracted {len(sales_data)} sales records")
            return sales_data
        except Exception as e:
            logger.error(f"Error extracting sales data: {str(e)}")
            raise

    def transform_sales_data(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Transform sales data."""
        try:
            # Calculate derived metrics
            sales_df['total_amount'] = sales_df['quantity'] * sales_df['unit_price']
            sales_df['profit'] = sales_df['total_amount'] - (sales_df['quantity'] * sales_df['unit_cost'])
            
            # Add time dimensions
            sales_df['year'] = sales_df['date'].dt.year
            sales_df['month'] = sales_df['date'].dt.month
            sales_df['day'] = sales_df['date'].dt.day
            sales_df['day_of_week'] = sales_df['date'].dt.dayofweek
            
            logger.info("Transformed sales data successfully")
            return sales_df
        except Exception as e:
            logger.error(f"Error transforming sales data: {str(e)}")
            raise

    def load_to_warehouse(self, df: pd.DataFrame, table_name: str) -> None:
        """Load transformed data into the data warehouse."""
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Loaded {len(df)} records to {table_name}")
        except Exception as e:
            logger.error(f"Error loading data to warehouse: {str(e)}")
            raise

    def run_daily_etl(self):
        """Execute the daily ETL process."""
        try:
            start_date = datetime.now() - timedelta(days=1)
            
            # Extract
            sales_data = self.extract_sales_data(start_date)
            
            # Transform
            transformed_sales = self.transform_sales_data(sales_data)
            
            # Load
            self.load_to_warehouse(transformed_sales, 'fact_sales')
            
            logger.info("Daily ETL process completed successfully")
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            raise

if __name__ == "__main__":
    # Create config directory and file if they don't exist
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    
    config_path = config_dir / "config.yml"
    if not config_path.exists():
        default_config = {
            'warehouse': {
                'connection_string': 'sqlite:///ecommerce.db'
            },
            'source_paths': {
                'sales': 'data/sales.csv',
                'customers': 'data/customers.csv',
                'products': 'data/products.csv'
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(default_config, f)

    # Initialize and run ETL
    etl = EcommerceETL(str(config_path))
    etl.run_daily_etl()