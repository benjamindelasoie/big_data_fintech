# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Big Data course project focused on building an ETL pipeline using Spark and Cassandra to analyze user onboarding metrics for a Latin American Fintech company. The project aims to reduce user churn by implementing a 30-day onboarding strategy and measuring its effectiveness through A/B testing.

## Business Context

**Problem**: A Fintech company is losing users in their first days of using the application due to cognitive overload from too many services.

**Solution**: Implement a 30-day onboarding process that guides new users to create banking products while reducing cognitive load on the home page.

**Experiment**: A/B testing in Brazil with 5% control group and 95% treatment group over 6 months.

## Dependencies and Environment Setup

Install dependencies:
```bash
pip install -r requirements.txt
```

Key dependencies:
- **PySpark** (>=3.3.0) - Main data processing framework
- **AstraDB** (>=1.0.0) - Database connectivity for Cassandra/DataStax Astra

## Business Metrics

The project measures 4 key onboarding metrics:

1. **Drop**: Percentage of users who don't return after viewing home page on registration day
2. **Activation**: Percentage of onboarding users who perform first transactional action
3. **Habit**: Percentage of onboarding users who complete 5 transactions:
   - **Individuals (segment 1)**: 5 transactions on 5 different days within 30-day onboarding
   - **Sellers (segment 2)**: 5 collection transactions (types 8-9) within 30-day onboarding
   - **Unknown segment**: Default to Individuals rule (5 transactions in 5 days)
4. **Setup**: Percentage of users who perform at least one setup action (add credit card, activate PIX, etc.)

## Data Structure

The project works with three main datasets located in the `data/` directory:

1. **bt_users_transactions.csv** - User transaction data:
   - `user_id`: User identifier (MLB prefix for Brazil users)
   - `transaction_dt`: Transaction timestamp
   - `type`: Transaction type (1-7 for payments, 8-9 for collections)
   - `segment`: User segment (1=individuals, 2=sellers)

2. **dim_users.csv** - User dimension data (equivalent to lk_users mentioned in requirements):
   - `user_id`: User identifier (MLB format)
   - `name`: User name
   - `email`: User email
   - `address`: User address
   - `birth_dt`: Birth date
   - `phone`: Phone number
   - `type`: User type (numeric) - equivalent to mentioned lk_users dataset
   - `rubro`: Business sector identifier (for sellers only)

3. **lk_onboarding.csv** - User onboarding tracking:
   - `first_login_dt`: First login timestamp
   - `week_year`: Week of year
   - `user_id`: User identifier
   - `habito`: Habit formation flag (1=achieved, 0=not achieved)
   - `habito_dt`: Date when habit was achieved
   - `activacion`: Activation flag (1=achieved, 0=not achieved)
   - `activacion_dt`: Date when activation occurred
   - `setup`: Setup completion flag (1=achieved, 0=not achieved)
   - `setup_dt`: Date when setup was completed
   - `return`: Return user flag (1=returned after first login, 0=did not return)

## Development Commands

Since this is a PySpark project, typical development commands would be:

```bash
# Run PySpark applications
spark-submit your_script.py

# Interactive PySpark shell
pyspark

# Python execution for data analysis
python your_analysis.py
```

## ETL Pipeline Requirements

The project requires implementing an ETL pipeline with the following stages:

### 1. Pre-processing Stage
- **Data Quality Analysis**: Identify unnecessary columns, missing values, and data inconsistencies
- **Data Cleaning**: Remove or fix data that doesn't align with business logic
- **Data Validation**: Ensure data consistency across all datasets
- **Output**: Clean, consistent datasets ready for analytical processing

### 2. Transformation Stage
- **Data Preparation**: Transform data for data scientists and analysts
- **Metric Calculation**: Calculate the 4 key business metrics (Drop, Activation, Habit, Setup)
- **A/B Testing Analysis**: Analyze performance of treatment group vs control group
- **Output**: Analytics-ready datasets for onboarding performance analysis

### 3. Visualization Stage (Optional)
- **Funnel Analysis**: Create visualizations showing user drop-off at each onboarding stage
- **Performance Dashboards**: Display key metrics and A/B testing results

## Architecture Notes

- **Big Data Architecture**: Built on Spark for distributed processing and Cassandra for storage
- **ETL Pipeline**: Multi-stage pipeline with preprocessing, transformation, and optional visualization
- **Data Integration**: Joins three datasets on `user_id` for comprehensive user journey analysis
- **A/B Testing Focus**: Specifically designed to measure onboarding experiment effectiveness
- **Brazilian Market**: User IDs with MLB prefix indicate focus on Brazilian users

## Data Analysis Patterns

Key analytical patterns for this project:
- **User Journey Analysis**: Track users from registration through onboarding milestones
- **Segmentation Analysis**: Different behavior patterns between individuals, sellers, and unknown segments
- **Cohort Analysis**: Compare A/B testing groups (5% control vs 95% treatment)
- **Funnel Analysis**: Measure conversion rates at each onboarding stage
- **Time-series Analysis**: Track user behavior over the 30-day onboarding period

## Important Implementation Notes

Based on teacher clarifications:

1. **User Segmentation Logic**:
   - New users without transactions have no segment assigned (normal behavior)
   - Users without transactions during onboarding are classified as "Unknown" segment
   - For habit calculation, Unknown segment users default to Individuals rule

2. **Dataset Mapping**:
   - `dim_users.csv` is equivalent to the `lk_users` dataset mentioned in requirements
   - `type` field in dim_users is equivalent to user type classification
   - `rubro` field only applies to sellers, not individuals

3. **Transaction-based Classification**:
   - **Individuals**: Users who only make payment transactions (types 1-7)
   - **Sellers**: Users who make collection transactions (types 8-9)
   - **Unknown**: Users with no transactions during onboarding period

## Clarifications about the project requirements made by the subject teacher

1. Sobre el dataset lk_users: La consigna menciona que existe un dataset lk_users con información de usuarios que incluye:
- user_id: identificador único de usuario
- rubro: identificador de rubro de negocio para usuarios del segmento seller
Pregunta: ¿El dataset lk_users debe contener información de segmentación directa (individuals vs sellers) o solo el campo rubro para sellers? En nuestro dataset actual tenemos dim_users.csv (en vez de lk_users.csv) con campo type - ¿es este el equivalente?
Respuesta: Solo campo rubro. Si, el campo type es equivalente.

2. Sobre la segmentación en transacciones: La consigna dice que en bt_users_transactions hay un campo segment donde:
1 = individuals
2 = sellers
Pregunta: ¿Todos los usuarios de onboarding deberían tener un segmento definido en algún dataset, o es normal que usuarios sin transacciones no tengan segmento asignado?
Respuesta: Es normal que usuarios nuevos no tengan segmento asignado.

3. Sobre la definición de Individuals vs Sellers: La consigna dice:
"Los individuals son usuarios que realizan sólo transacciones de pago"
"Los sellers también realizan transacciones de cobro"
Pregunta: ¿Un usuario que no tiene transacciones durante el período de onboarding cómo se clasifica? ¿Se considera "Individual" por defecto o queda como "Unknown"?
Respuesta: Queda Unknown

4. Sobre la métrica de Hábito: Las reglas son claras pero surge la duda:
Pregunta: ¿Para calcular la métrica de hábito de un usuario sin segmento definido, se debe aplicar la regla de Individuals (5 transacciones en 5 días) como valor por defecto?
Respuesta: Si. gracias por aclararlo. Por defecto se toma como individuals.

## END OF CLARIFICATIONS

## ⚠️ CRITICAL DATA PARSING CORRECTION

**IMPORTANT**: During implementation, a critical CSV parsing issue was discovered and corrected:

### The Problem
- The `dim_users.csv` file contains multiline address fields with embedded newlines
- Standard CSV parsing counted **39,000 lines** as records
- **Reality**: Only **13,000 actual user records** exist in the dataset

### The Solution
All ETL pipeline components have been updated with **proper CSV parsing**:

```python
# INCORRECT (Original)
df = spark.read.csv(path, header=True, inferSchema=True)

# CORRECT (Fixed)
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .csv(path)
```

### Impact on Business Metrics
All business metrics have been **recalculated with the correct 13,000 user baseline**:

**Corrected Realistic Metrics:**
- **Total Users**: 13,000 (not 39,000)
- **Transaction Coverage**: 11.8% of users have transactions (1,531 users)
- **Unknown Segment**: 88.2% of users (11,469 users with no transactions)
- **Drop Rate**: 26.5% of users don't return after registration
- **Activation Rate**: 48.5% of users perform first transaction
- **Setup Rate**: 44.0% of users complete setup actions  
- **Habit Rate**: 12.3% of users achieve sustained usage habits

## Corrected ETL Pipeline Implementation

The project now includes a **complete corrected ETL pipeline** with proper CSV parsing and realistic business metrics.

### Pipeline Components

1. **Corrected Preprocessing** (`src/preprocessing/corrected_data_preprocessing.py`)
   - Fixed CSV parsing for multiline address fields
   - Proper data validation and cleaning
   - Outputs clean datasets with correct user counts

2. **Corrected Transformation** (`src/transformation/corrected_data_transformation.py`)
   - User segmentation with realistic ratios
   - Business metrics calculation with proper baselines
   - Comprehensive user journey analysis

3. **Corrected Loading** (`src/loading/corrected_data_loading.py`)
   - Analytics data storage with Cassandra fallback
   - Time-series aggregations (monthly/weekly)
   - Funnel analysis tables

### Running the Corrected Pipeline

**Always use the virtual environment:**

```bash
# Simple execution (recommended)
venv/bin/python run_corrected_pipeline.py

# Advanced execution with options
venv/bin/python run_complete_pipeline.py [--skip-exploration] [--stage STAGE]

# Individual corrected stages
venv/bin/python src/preprocessing/corrected_data_preprocessing.py
venv/bin/python src/transformation/corrected_data_transformation.py
venv/bin/python src/loading/corrected_data_loading.py
```

### Expected Output Files

**Core Processed Data:**
- `processed_data/clean_users.parquet` (12,000 records after cleaning)
- `processed_data/clean_transactions.parquet` (7,655 records)
- `processed_data/clean_onboarding.parquet` (13,500 records)

**Analytics Ready Data:**
- `processed_data/user_metrics.parquet` (1,531 users with transactions)
- `processed_data/final_analytics.parquet` (13,000 total users)

**Cassandra Fallback Files:**
- `processed_data/corrected_cassandra_fallback_*.parquet` (various analytics tables)

### Key Business Insights (Corrected)

1. **Realistic User Engagement**: Only 11.8% of onboarding users have transactions
2. **Large Improvement Opportunity**: 88.2% of users never transact during onboarding
3. **Strong Conversion Among Active Users**: 66.8% activation rate among returned users
4. **Reasonable Funnel Progression**: 44% setup completion, 12.3% habit formation

### Documentation

- **`CORRECTED_ETL_PIPELINE_SUMMARY.md`**: Comprehensive documentation of the corrected implementation
- **`run_corrected_pipeline.py`**: Simple pipeline runner script
- **`run_complete_pipeline.py`**: Advanced pipeline runner with options

## AstraDB Cloud Database Integration

The project is now **fully configured for AstraDB cloud database persistence**:

### Setup Status
✅ **AstraDB Connection**: Configured and tested  
✅ **Credentials**: Securely stored in `.env` file  
✅ **Secure Bundle**: Located at `astradb/secure-connect-big-data.zip`  
✅ **ETL Pipeline**: Ready for cloud data persistence  

### AstraDB Configuration Details
- **Database**: big_data  
- **Region**: eu-west-1  
- **Keyspace**: fintech_analytics  
- **Tables**: 5 analytics tables (user_segments, user_metrics, funnel_analysis, monthly_metrics, weekly_metrics)

### Running ETL with AstraDB
```bash
# Complete ETL pipeline with AstraDB persistence
source venv/bin/activate && python run_astradb_pipeline.py

# Alternative: Standard pipeline (uses Parquet fallback)
source venv/bin/activate && python run_corrected_pipeline.py
```

### AstraDB Table Setup
**IMPORTANT**: Before running the ETL pipeline, tables must be created in AstraDB:

1. **Generate CQL statements**: `python setup_astradb_tables.py`
2. **Go to AstraDB Console**: https://astra.datastax.com/
3. **Open CQL Console** in your 'big_data' database
4. **Execute the provided CQL statements** to create tables
5. **Run ETL pipeline** to load data

### Expected Results
When AstraDB is properly configured:
- ✅ **13,000 user records** loaded to user_metrics table
- ✅ **1,531 transaction users** loaded to user_segments table  
- ✅ **Funnel analysis** with realistic conversion rates
- ✅ **Time-series data** in monthly/weekly metrics tables
- ✅ **Real-time querying** through AstraDB console

### Troubleshooting AstraDB
If tables are not created in AstraDB console:
- Pipeline automatically falls back to Parquet files (`astradb_fallback_*.parquet`)
- Data processing continues normally
- Results available locally until AstraDB setup is completed

## Important Notes for Future Claude Code Instances

1. **Always use multiLine=True** when reading the users CSV file
2. **Use virtual environment** for all Python script execution: `source venv/bin/activate`
3. **Expect ~13,000 users** as the baseline, not 39,000
4. **Transaction coverage is low** (~11.8%) which is realistic for onboarding analysis
5. **Pipeline execution takes ~60 seconds** for complete processing
6. **AstraDB integration** is fully configured - just need tables created in console
7. **Environment variables** are stored in `.env` file for AstraDB credentials
8. **Fallback mechanism** ensures pipeline works even without AstraDB connection
