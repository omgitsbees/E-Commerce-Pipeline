# Enhanced E-commerce ETL Pipeline

A robust, production-ready ETL (Extract, Transform, Load) pipeline for e-commerce data processing with comprehensive monitoring, data quality validation, and enterprise-grade features.

## Overview

This ETL pipeline processes e-commerce sales data with advanced transformation capabilities, data quality checks, and comprehensive monitoring. It supports multiple data sources, flexible loading strategies, and includes notification systems for operational monitoring.

## Features

### Core Functionality
- **Extract**: Support for CSV files and database sources
- **Transform**: Advanced business logic with derived metrics and dimensions
- **Load**: Multiple loading strategies (append, truncate, upsert)
- **Scheduling**: Daily ETL execution with configurable date ranges

### Enterprise Features
- **Data Quality Validation**: Comprehensive validation using Great Expectations
- **Monitoring & Metrics**: Detailed execution tracking and performance metrics
- **Notification System**: Email alerts for success/failure scenarios
- **Error Handling**: Robust exception handling with detailed logging
- **Backfill Support**: Historical data processing capabilities
- **Configuration Management**: Flexible YAML-based configuration

## Installation

### Prerequisites
- Python 3.7+
- SQLAlchemy-supported database (SQLite, PostgreSQL, MySQL, etc.)

### Dependencies
```bash
pip install pandas numpy sqlalchemy pyyaml great-expectations
```

### Optional Dependencies
For email notifications:
```bash
pip install secure-smtplib
```

## Quick Start

1. **Clone the repository**
```bash
git clone <repository-url>
cd ecommerce-etl-pipeline
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Create data directory and sample data**
```bash
mkdir -p data
# Add your sales.csv, customers.csv, products.csv files
```

4. **Run the pipeline**
```bash
python ecommerce_etl.py
```

## Configuration

The pipeline uses a YAML configuration file (`config/config.yml`) with the following structure:

```yaml
warehouse:
  connection_string: 'sqlite:///ecommerce_enhanced.db'
  load_strategy: 'append'  # append, truncate_and_load, upsert

source_paths:
  sales:
    type: 'csv'
    path: 'data/sales.csv'
  customers: 'data/customers.csv'
  products: 'data/products.csv'

data_quality:
  required_columns: ['date', 'customer_id', 'product_id', 'quantity', 'unit_price']
  data_types:
    quantity: 'int'
    unit_price: 'float'
  value_ranges:
    quantity: {min: 0, max: 1000}
    unit_price: {min: 0, max: 10000}

notifications:
  enabled: false
  email:
    smtp_server: 'smtp.gmail.com'
    from_email: 'your-email@gmail.com'
    to_email: 'admin@yourcompany.com'
```

## Usage

### Basic Usage
```python
from ecommerce_etl import EcommerceETL

# Initialize ETL pipeline
etl = EcommerceETL('config/config.yml')

# Run daily ETL
etl.run_daily_etl()
```

### Custom Date Range
```python
from datetime import datetime

etl.run_daily_etl(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31)
)
```

### Historical Backfill
```python
etl.run_backfill(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31),
    batch_days=30
)
```

## Data Schema

### Input Data Requirements
The pipeline expects sales data with the following minimum schema:
- `date`: Transaction date
- `customer_id`: Customer identifier
- `product_id`: Product identifier
- `quantity`: Quantity sold
- `unit_price`: Price per unit
- `unit_cost`: Cost per unit

### Output Schema
The transformed data includes additional calculated fields:
- `total_amount`: quantity × unit_price
- `profit`: total_amount - (quantity × unit_cost)
- `profit_margin`: (profit / total_amount) × 100
- Time dimensions: `year`, `month`, `day`, `quarter`, `season`
- `customer_tier`: Bronze, Silver, Gold, Platinum
- `is_weekend`: Boolean flag for weekend transactions
- ETL metadata: `etl_created_at`, `etl_batch_id`

## Monitoring and Logging

### Logging
- Comprehensive logging to both file (`etl_pipeline.log`) and console
- Structured log format with timestamps, function names, and line numbers
- Separate tracking for INFO, WARNING, and ERROR levels

### Metrics Tracking
The pipeline tracks the following metrics:
- Execution duration
- Records extracted, transformed, and loaded
- Error and warning counts
- Data quality validation results

### ETL Metadata
All ETL runs are logged in the `etl_metadata` table with:
- Run timestamps
- Processing status
- Record counts
- Error details

## Data Quality

### Validation Rules
- **Completeness**: Required columns validation
- **Data Types**: Type checking for numeric fields
- **Value Ranges**: Min/max validation for quantities and prices
- **Business Rules**: Custom validation (future dates, negative values)

### Quality Reporting
- Data quality issues are logged as warnings
- Detailed quality reports in ETL metrics
- Configurable quality thresholds

## Notification System

### Email Notifications
- Success notifications with execution summary
- Failure notifications with error details
- Configurable SMTP settings
- HTML and plain text formats

### Notification Content
- Execution metrics and duration
- Record processing counts
- Error and warning summaries
- Detailed error messages for failures

## Architecture

### Class Structure
- `EcommerceETL`: Main ETL orchestrator
- `ETLMetrics`: Metrics tracking and reporting
- `DataQualityValidator`: Data validation logic
- `NotificationService`: Alert and notification handling

### Design Patterns
- Context managers for operation tracking
- Dataclass for structured metrics
- Abstract base classes for extensibility
- Configuration-driven architecture

## What's New: Enhanced vs Original

This enhanced version includes significant improvements over the original pipeline:

### New Features Added

#### 1. **Comprehensive Monitoring System**
- **Original**: Basic logging with simple INFO messages
- **Enhanced**: Detailed metrics tracking with `ETLMetrics` class, execution timing, record counts, and structured error/warning collection

#### 2. **Data Quality Validation**
- **Original**: No data quality checks
- **Enhanced**: Full validation framework using Great Expectations with configurable rules for completeness, data types, value ranges, and business logic

#### 3. **Advanced Error Handling**
- **Original**: Basic try-catch blocks with simple error messages
- **Enhanced**: Context managers for operation tracking, comprehensive exception handling, detailed error logging, and graceful failure recovery

#### 4. **Enhanced Transformations**
- **Original**: Basic calculations (total_amount, profit) and simple time dimensions
- **Enhanced**: Advanced business logic including profit margins, customer segmentation, seasonal analysis, weekend classification, and ETL batch tracking

#### 5. **Flexible Loading Strategies**
- **Original**: Simple append-only loading
- **Enhanced**: Multiple strategies (append, truncate_and_load, upsert) with chunked loading for performance

#### 6. **Notification System**
- **Original**: No notification capabilities
- **Enhanced**: Email notification system with success/failure alerts, detailed metrics reporting, and configurable SMTP settings

#### 7. **Configuration Management**
- **Original**: Simple YAML config with basic database and file paths
- **Enhanced**: Comprehensive configuration including data quality rules, notification settings, loading strategies, and source type definitions

#### 8. **Operational Features**
- **Original**: Single daily ETL execution
- **Enhanced**: Backfill functionality, batch processing, custom date ranges, and ETL run metadata tracking

#### 9. **Database Schema Management**
- **Original**: No schema initialization
- **Enhanced**: Automatic warehouse initialization, metadata table creation, and schema management

#### 10. **Code Organization**
- **Original**: Single class with basic methods
- **Enhanced**: Multiple specialized classes, design patterns, type hints, and comprehensive documentation

### Performance Improvements
- Chunked database loading for large datasets
- Optimized memory usage with data copying strategies
- Batch processing for historical data loads
- Connection pooling and transaction management

### Maintainability Enhancements
- Comprehensive type hints and documentation
- Structured configuration with validation
- Modular design with separation of concerns
- Extensive logging for debugging and monitoring

### Production Readiness
- Enterprise-grade error handling and recovery
- Monitoring and alerting capabilities
- Data quality assurance
- Operational metadata tracking
- Scalable architecture for future enhancements

## License

This project is licensed under the MIT License - see the LICENSE file for details.
