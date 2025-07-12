import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
from sqlalchemy import create_engine, text
import yaml
import os
from pathlib import Path
from dataclasses import dataclass
from abc import ABC, abstractmethod
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import hashlib
import time
from contextlib import contextmanager
import great_expectations as ge
from great_expectations.dataset import PandasDataset

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ecommerce_etl')

@dataclass
class ETLMetrics:
    """Container for ETL execution metrics."""
    start_time: datetime
    end_time: Optional[datetime] = None
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    errors: List[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
    
    @property
    def duration(self) -> Optional[timedelta]:
        if self.end_time:
            return self.end_time - self.start_time
        return None
    
    def to_dict(self) -> Dict:
        return {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration.total_seconds() if self.duration else None,
            'records_extracted': self.records_extracted,
            'records_transformed': self.records_transformed,
            'records_loaded': self.records_loaded,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'errors': self.errors,
            'warnings': self.warnings
        }

class DataQualityValidator:
    """Handles data quality validation using Great Expectations."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.quality_rules = config.get('data_quality', {})
    
    def validate_sales_data(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate sales data quality."""
        issues = []
        
        # Convert to Great Expectations dataset
        ge_df = PandasDataset(df)
        
        try:
            # Basic completeness checks
            if 'required_columns' in self.quality_rules:
                for col in self.quality_rules['required_columns']:
                    if col not in df.columns:
                        issues.append(f"Missing required column: {col}")
                    elif df[col].isna().sum() > 0:
                        issues.append(f"Null values found in required column: {col}")
            
            # Data type validation
            if 'data_types' in self.quality_rules:
                for col, expected_type in self.quality_rules['data_types'].items():
                    if col in df.columns:
                        if not df[col].dtype.name.startswith(expected_type):
                            issues.append(f"Column {col} has incorrect data type")
            
            # Value range validation
            if 'value_ranges' in self.quality_rules:
                for col, range_config in self.quality_rules['value_ranges'].items():
                    if col in df.columns:
                        min_val = range_config.get('min')
                        max_val = range_config.get('max')
                        if min_val is not None and (df[col] < min_val).any():
                            issues.append(f"Column {col} has values below minimum: {min_val}")
                        if max_val is not None and (df[col] > max_val).any():
                            issues.append(f"Column {col} has values above maximum: {max_val}")
            
            # Custom business rules
            self._validate_business_rules(df, issues)
            
        except Exception as e:
            issues.append(f"Data quality validation error: {str(e)}")
        
        return len(issues) == 0, issues
    
    def _validate_business_rules(self, df: pd.DataFrame, issues: List[str]):
        """Validate business-specific rules."""
        # Example: Check for reasonable dates
        if 'date' in df.columns:
            future_dates = df[df['date'] > datetime.now()]
            if len(future_dates) > 0:
                issues.append(f"Found {len(future_dates)} records with future dates")
        
        # Example: Check for negative quantities
        if 'quantity' in df.columns:
            negative_qty = df[df['quantity'] < 0]
            if len(negative_qty) > 0:
                issues.append(f"Found {len(negative_qty)} records with negative quantities")
        
        # Example: Check for zero prices
        if 'unit_price' in df.columns:
            zero_prices = df[df['unit_price'] <= 0]
            if len(zero_prices) > 0:
                issues.append(f"Found {len(zero_prices)} records with zero or negative prices")

class NotificationService:
    """Handles ETL notifications and alerts."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.enabled = config.get('notifications', {}).get('enabled', False)
        self.email_config = config.get('notifications', {}).get('email', {})
    
    def send_success_notification(self, metrics: ETLMetrics):
        """Send success notification."""
        if not self.enabled:
            return
        
        subject = f"ETL Pipeline Success - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        body = f"""
        ETL Pipeline completed successfully!
        
        Metrics:
        - Duration: {metrics.duration}
        - Records Extracted: {metrics.records_extracted:,}
        - Records Transformed: {metrics.records_transformed:,}
        - Records Loaded: {metrics.records_loaded:,}
        - Warnings: {len(metrics.warnings)}
        """
        
        self._send_email(subject, body)
    
    def send_failure_notification(self, metrics: ETLMetrics, error: Exception):
        """Send failure notification."""
        if not self.enabled:
            return
        
        subject = f"ETL Pipeline FAILED - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        body = f"""
        ETL Pipeline failed with error: {str(error)}
        
        Metrics:
        - Duration: {metrics.duration}
        - Records Extracted: {metrics.records_extracted:,}
        - Records Transformed: {metrics.records_transformed:,}
        - Records Loaded: {metrics.records_loaded:,}
        - Errors: {len(metrics.errors)}
        - Warnings: {len(metrics.warnings)}
        
        Error Details:
        {chr(10).join(metrics.errors)}
        """
        
        self._send_email(subject, body)
    
    def _send_email(self, subject: str, body: str):
        """Send email notification."""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config.get('from_email')
            msg['To'] = self.email_config.get('to_email')
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.email_config.get('smtp_server'), 587)
            server.starttls()
            server.login(self.email_config.get('username'), self.email_config.get('password'))
            server.send_message(msg)
            server.quit()
            
            logger.info("Notification email sent successfully")
        except Exception as e:
            logger.error(f"Failed to send notification email: {str(e)}")

class EcommerceETL:
    """Enhanced ETL pipeline with comprehensive features."""
    
    def __init__(self, config_path: str = 'config/config.yml'):
        """Initialize ETL pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.engine = create_engine(self.config['warehouse']['connection_string'])
        self.validator = DataQualityValidator(self.config)
        self.notifier = NotificationService(self.config)
        self.metrics = ETLMetrics(start_time=datetime.now())
        
        # Initialize warehouse tables
        self._initialize_warehouse()
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration: {str(e)}")
            raise
    
    def _initialize_warehouse(self):
        """Initialize warehouse tables and metadata."""
        try:
            # Create metadata table for tracking ETL runs
            metadata_sql = """
            CREATE TABLE IF NOT EXISTS etl_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date DATE,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status VARCHAR(20),
                records_processed INTEGER,
                errors TEXT,
                warnings TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(metadata_sql))
                conn.commit()
            
            logger.info("Warehouse initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing warehouse: {str(e)}")
            raise
    
    @contextmanager
    def _track_operation(self, operation_name: str):
        """Context manager for tracking operation metrics."""
        start_time = time.time()
        logger.info(f"Starting {operation_name}")
        
        try:
            yield
            duration = time.time() - start_time
            logger.info(f"Completed {operation_name} in {duration:.2f} seconds")
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Failed {operation_name} after {duration:.2f} seconds: {str(e)}")
            raise
    
    def extract_sales_data(self, start_date: datetime, end_date: Optional[datetime] = None) -> pd.DataFrame:
        """Extract sales data from source system with enhanced error handling."""
        with self._track_operation("sales data extraction"):
            try:
                # Support multiple source types
                source_config = self.config['source_paths']['sales']
                
                if isinstance(source_config, str):
                    # Simple CSV file
                    sales_data = pd.read_csv(source_config)
                elif isinstance(source_config, dict):
                    # Database or advanced source
                    if source_config.get('type') == 'database':
                        query = source_config.get('query', 'SELECT * FROM sales')
                        sales_data = pd.read_sql(query, self.engine)
                    else:
                        sales_data = pd.read_csv(source_config['path'])
                else:
                    raise ValueError("Invalid source configuration")
                
                # Data type conversions
                sales_data['date'] = pd.to_datetime(sales_data['date'])
                
                # Date filtering
                if end_date is None:
                    end_date = datetime.now()
                
                sales_data = sales_data[
                    (sales_data['date'] >= start_date) & 
                    (sales_data['date'] <= end_date)
                ]
                
                self.metrics.records_extracted = len(sales_data)
                logger.info(f"Extracted {len(sales_data)} sales records from {start_date} to {end_date}")
                
                return sales_data
                
            except Exception as e:
                error_msg = f"Error extracting sales data: {str(e)}"
                logger.error(error_msg)
                self.metrics.errors.append(error_msg)
                raise
    
    def transform_sales_data(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Transform sales data with comprehensive business logic."""
        with self._track_operation("sales data transformation"):
            try:
                # Data quality validation
                is_valid, quality_issues = self.validator.validate_sales_data(sales_df)
                
                if not is_valid:
                    for issue in quality_issues:
                        self.metrics.warnings.append(issue)
                        logger.warning(f"Data quality issue: {issue}")
                
                # Create a copy to avoid modifying original data
                transformed_df = sales_df.copy()
                
                # Basic calculations
                transformed_df['total_amount'] = transformed_df['quantity'] * transformed_df['unit_price']
                transformed_df['profit'] = transformed_df['total_amount'] - (transformed_df['quantity'] * transformed_df['unit_cost'])
                transformed_df['profit_margin'] = (transformed_df['profit'] / transformed_df['total_amount']) * 100
                
                # Time dimensions
                transformed_df['year'] = transformed_df['date'].dt.year
                transformed_df['month'] = transformed_df['date'].dt.month
                transformed_df['day'] = transformed_df['date'].dt.day
                transformed_df['day_of_week'] = transformed_df['date'].dt.dayofweek
                transformed_df['week_of_year'] = transformed_df['date'].dt.isocalendar().week
                transformed_df['quarter'] = transformed_df['date'].dt.quarter
                
                # Advanced transformations
                transformed_df['is_weekend'] = transformed_df['day_of_week'].isin([5, 6])
                transformed_df['season'] = transformed_df['month'].map({
                    12: 'Winter', 1: 'Winter', 2: 'Winter',
                    3: 'Spring', 4: 'Spring', 5: 'Spring',
                    6: 'Summer', 7: 'Summer', 8: 'Summer',
                    9: 'Fall', 10: 'Fall', 11: 'Fall'
                })
                
                # Customer segmentation (example)
                if 'customer_id' in transformed_df.columns:
                    customer_stats = transformed_df.groupby('customer_id').agg({
                        'total_amount': 'sum',
                        'order_id': 'count'
                    }).reset_index()
                    
                    customer_stats['avg_order_value'] = customer_stats['total_amount'] / customer_stats['order_id']
                    customer_stats['customer_tier'] = pd.cut(
                        customer_stats['total_amount'], 
                        bins=[0, 100, 500, 1000, float('inf')], 
                        labels=['Bronze', 'Silver', 'Gold', 'Platinum']
                    )
                    
                    transformed_df = transformed_df.merge(
                        customer_stats[['customer_id', 'customer_tier']], 
                        on='customer_id', 
                        how='left'
                    )
                
                # Add ETL metadata
                transformed_df['etl_created_at'] = datetime.now()
                transformed_df['etl_batch_id'] = self._generate_batch_id()
                
                # Handle missing values
                numeric_columns = transformed_df.select_dtypes(include=[np.number]).columns
                transformed_df[numeric_columns] = transformed_df[numeric_columns].fillna(0)
                
                self.metrics.records_transformed = len(transformed_df)
                logger.info(f"Transformed {len(transformed_df)} sales records")
                
                return transformed_df
                
            except Exception as e:
                error_msg = f"Error transforming sales data: {str(e)}"
                logger.error(error_msg)
                self.metrics.errors.append(error_msg)
                raise
    
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID for ETL run."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        random_hash = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        return f"batch_{timestamp}_{random_hash}"
    
    def load_to_warehouse(self, df: pd.DataFrame, table_name: str, load_strategy: str = 'append') -> None:
        """Load transformed data into the data warehouse with multiple strategies."""
        with self._track_operation(f"loading to {table_name}"):
            try:
                if load_strategy == 'truncate_and_load':
                    # Truncate table first
                    with self.engine.connect() as conn:
                        conn.execute(text(f"DELETE FROM {table_name}"))
                        conn.commit()
                    
                    df.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists='append',
                        index=False,
                        chunksize=1000
                    )
                
                elif load_strategy == 'upsert':
                    # Implement upsert logic (simplified for SQLite)
                    df.to_sql(
                        name=f"{table_name}_temp",
                        con=self.engine,
                        if_exists='replace',
                        index=False
                    )
                    
                    # Merge logic would go here (database-specific)
                    with self.engine.connect() as conn:
                        conn.execute(text(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM {table_name}_temp"))
                        conn.execute(text(f"DROP TABLE {table_name}_temp"))
                        conn.commit()
                
                else:  # append
                    df.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists='append',
                        index=False,
                        chunksize=1000
                    )
                
                self.metrics.records_loaded = len(df)
                logger.info(f"Loaded {len(df)} records to {table_name} using {load_strategy} strategy")
                
            except Exception as e:
                error_msg = f"Error loading data to warehouse: {str(e)}"
                logger.error(error_msg)
                self.metrics.errors.append(error_msg)
                raise
    
    def _log_etl_run(self, status: str):
        """Log ETL run metadata."""
        try:
            metadata = {
                'run_date': datetime.now().date(),
                'start_time': self.metrics.start_time,
                'end_time': self.metrics.end_time,
                'status': status,
                'records_processed': self.metrics.records_loaded,
                'errors': json.dumps(self.metrics.errors),
                'warnings': json.dumps(self.metrics.warnings)
            }
            
            pd.DataFrame([metadata]).to_sql(
                'etl_metadata',
                con=self.engine,
                if_exists='append',
                index=False
            )
            
        except Exception as e:
            logger.error(f"Error logging ETL run metadata: {str(e)}")
    
    def run_daily_etl(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
        """Execute the daily ETL process with comprehensive monitoring."""
        try:
            if start_date is None:
                start_date = datetime.now() - timedelta(days=1)
            
            logger.info(f"Starting daily ETL process for {start_date.strftime('%Y-%m-%d')}")
            
            # Extract
            sales_data = self.extract_sales_data(start_date, end_date)
            
            if len(sales_data) == 0:
                logger.warning("No data to process for the specified date range")
                return
            
            # Transform
            transformed_sales = self.transform_sales_data(sales_data)
            
            # Load
            load_strategy = self.config.get('warehouse', {}).get('load_strategy', 'append')
            self.load_to_warehouse(transformed_sales, 'fact_sales', load_strategy)
            
            # Finalize metrics
            self.metrics.end_time = datetime.now()
            
            # Log successful run
            self._log_etl_run('SUCCESS')
            
            # Send success notification
            self.notifier.send_success_notification(self.metrics)
            
            logger.info(f"Daily ETL process completed successfully in {self.metrics.duration}")
            logger.info(f"Final metrics: {self.metrics.to_dict()}")
            
        except Exception as e:
            self.metrics.end_time = datetime.now()
            self._log_etl_run('FAILED')
            self.notifier.send_failure_notification(self.metrics, e)
            logger.error(f"ETL process failed: {str(e)}")
            raise
    
    def run_backfill(self, start_date: datetime, end_date: datetime, batch_days: int = 7):
        """Run backfill process for historical data."""
        logger.info(f"Starting backfill process from {start_date} to {end_date}")
        
        current_date = start_date
        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=batch_days), end_date)
            
            try:
                logger.info(f"Processing batch: {current_date} to {batch_end}")
                self.run_daily_etl(current_date, batch_end)
                current_date = batch_end + timedelta(days=1)
                
            except Exception as e:
                logger.error(f"Backfill failed for batch {current_date} to {batch_end}: {str(e)}")
                current_date = batch_end + timedelta(days=1)
                continue
        
        logger.info("Backfill process completed")

def create_enhanced_config():
    """Create enhanced configuration file."""
    config = {
        'warehouse': {
            'connection_string': 'sqlite:///ecommerce_enhanced.db',
            'load_strategy': 'append'  # append, truncate_and_load, upsert
        },
        'source_paths': {
            'sales': {
                'type': 'csv',
                'path': 'data/sales.csv'
            },
            'customers': 'data/customers.csv',
            'products': 'data/products.csv'
        },
        'data_quality': {
            'required_columns': ['date', 'customer_id', 'product_id', 'quantity', 'unit_price'],
            'data_types': {
                'quantity': 'int',
                'unit_price': 'float',
                'unit_cost': 'float'
            },
            'value_ranges': {
                'quantity': {'min': 0, 'max': 1000},
                'unit_price': {'min': 0, 'max': 10000}
            }
        },
        'notifications': {
            'enabled': False,
            'email': {
                'smtp_server': 'smtp.gmail.com',
                'from_email': 'your-email@gmail.com',
                'to_email': 'admin@yourcompany.com',
                'username': 'your-email@gmail.com',
                'password': 'your-app-password'
            }
        }
    }
    return config

if __name__ == "__main__":
    # Create config directory and enhanced config file
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    
    config_path = config_dir / "config.yml"
    if not config_path.exists():
        with open(config_path, 'w') as f:
            yaml.dump(create_enhanced_config(), f, default_flow_style=False)
        logger.info("Enhanced configuration file created")
    
    # Create sample data directory
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Initialize and run ETL
    try:
        etl = EcommerceETL(str(config_path))
        etl.run_daily_etl()
    except Exception as e:
        logger.error(f"ETL execution failed: {str(e)}")
        exit(1)