# 🚀 Enterprise ETL Data Validation Framework

A production-ready, PySpark-based automated testing framework for validating ETL pipelines with support for **SCD Type 1 & Type 2** transformations, multi-stage validation, and comprehensive data quality checks.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-orange.svg)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📋 Table of Contents
- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Validation Checks](#validation-checks)
- [Sample Output](#sample-output)
- [Use Cases](#use-cases)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)

---

## 🎯 Overview

This framework automates ETL pipeline testing by performing **10+ comprehensive validation checks** across different stages of data transformation. It validates data quality, integrity, and transformation logic from **Source → Staging → Target** with special handling for Slowly Changing Dimensions (SCD).

**Perfect for:**
- Data Engineers validating ETL pipelines
- QA teams automating data quality checks
- Organizations implementing data governance
- Anyone building reliable data pipelines

---

## ✨ Key Features

### 🔍 Comprehensive Validation Suite
- ✅ **Schema Validation** - Data types, nullable constraints, field presence
- ✅ **Count Validation** - Record count matching with batch-aware logic
- ✅ **Null Validation** - Null/blank detection in non-nullable columns
- ✅ **Primary Key Validation** - Duplicate detection
- ✅ **Data Completeness** - No record loss tracking
- ✅ **Data Consistency** - Column-by-column value comparison
- ✅ **Referential Integrity** - Foreign key validation with lookup tables
- ✅ **Transformation Validation** - Business rule verification

### 🎭 SCD Support
- **Type 1 (Overwrite):** Latest batch validation
- **Type 2 (History):** Active flag validation, historical record preservation

### 🔧 Flexible Configuration
- **JSON-based configuration** for schemas and file paths
- **Multi-mode validation:** Source→Staging, Staging→Target, or Both
- **Easy extensibility** for new datasets

### 📊 Rich Reporting
- Color-coded pass/fail indicators (✔️/❌)
- Detailed error messages with row-level diagnostics
- Summary statistics (total checks, passed, failed)

---

## 🏗️ Architecture

```
┌─────────────────┐
│  QA_Driver.py   │  ← Main Orchestrator
└────────┬────────┘
         │
         ├─────────────────────────────────┐
         │                                 │
         ▼                                 ▼
┌──────────────────┐           ┌──────────────────┐
│   LoadData.py    │           │ Configuration    │
│                  │           │  - file_config   │
│ • Loads CSVs     │           │  - schema_config │
│ • Creates DFs    │           └──────────────────┘
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ etl_validation.py│
│                  │
│ Etl_Val Class    │
│ • Schema checks  │
│ • Count checks   │
│ • Null checks    │
│ • PK validation  │
│ • Completeness   │
│ • Consistency    │
│ • SCD logic      │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Results         │
│  • Console       │
│  • Logs          │
│  • Reports       │
└──────────────────┘
```

---

## 📁 Project Structure

```
ETL_Test_Automation/
│
├── QA_Driver.py                 # Main execution script
├── etl_validation.py            # Core validation logic (Etl_Val class)
├── load_data.py                 # Data loading utilities (LoadData class)
├── file_creation_new.py         # Sample data generator
│
├── file_config.json             # File paths and validation rules
├── schema_config.json           # Expected schema definitions
│
├── customer_src.csv             # Sample: Customer source data
├── customer_staging.csv         # Sample: Customer staging data
├── customer_target.csv          # Sample: Customer target (SCD Type 1)
├── orders_source.csv            # Sample: Orders source data
├── orders_staging.csv           # Sample: Orders staging data
├── orders_target.csv            # Sample: Orders target (SCD Type 2)
│
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## 🔧 Installation

### Prerequisites
- Python 3.8+
- Java 11 or 17 (required for PySpark)
- Apache Spark 3.x

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/etl-validation-framework.git
cd etl-validation-framework

# Install dependencies
pip install -r requirements.txt

# Set JAVA_HOME (if not already set)
export JAVA_HOME=/path/to/java-17

# Generate sample data
python file_creation_new.py

# Run validation
python QA_Driver.py
```

### requirements.txt
```txt
pyspark==3.5.0
pandas==2.1.0
```

---

## 🚀 Quick Start

### 1. Run Interactive Validation

```bash
python QA_Driver.py
```

**You'll see:**
```
Available files for ETL validation:
1. customer_data
2. orders_data
Enter the number corresponding to the file you want to validate: 1

----------------------------------------------------------------------
Validation Mode Options:
----------------------------------------------------------------------
1. Source → Staging only
2. Staging → Target only
3. Both (Complete Pipeline)

Enter validation mode (1-3): 3
```

### 2. View Results

```
======================================================================
                 FINAL VALIDATION SUMMARY
======================================================================
FILE PROCESSED      : CUSTOMER_DATA
VALIDATION MODE     : both
----------------------------------------------------------------------

SOURCE → STAGING
----------------------------------------------------------------------
CHECK NAME                          STATUS     
----------------------------------------------------------------------
schemachk                           Success    ✔️
count_val                           Success    ✔️
null_val                            Success    ✔️
primary_key_val                     Fail       ❌
data_completeness                   Success    ✔️
data_consistency                    Success    ✔️
----------------------------------------------------------------------
TOTAL CHECKS : 6 | PASSED: 5 | FAILED: 1
======================================================================
```

---

## ⚙️ Configuration

### file_config.json

Defines file paths, primary keys, and validation rules:

```json
{
  "customer_data": {
    "src_file_path": "/path/to/customer_src.csv",
    "stg_file_path": "/path/to/customer_staging.csv",
    "trgt_file_path": "/path/to/customer_target.csv",
    "primary_key": ["customer_id"],
    "non_nullable_columns": ["customer_id", "batch_date"],
    "scd_type": "type1"
  },
  "orders_data": {
    "src_file_path": "/path/to/orders_source.csv",
    "stg_file_path": "/path/to/orders_staging.csv",
    "trgt_file_path": "/path/to/orders_target.csv",
    "primary_key": ["order_id"],
    "non_nullable": {
      "source_to_staging": ["order_id", "customer_id"],
      "staging_to_target": ["order_id", "customer_id", "is_active"]
    },
    "scd_type": "type2",
    "active_flag_column": "is_active",
    "lookup": {
      "lookup_file": "customer_data",
      "lookup_column": "customer_id",
      "target_column": "customer_id"
    }
  }
}
```

### schema_config.json

Defines expected schemas for staging and target:

```json
{
  "customer_data": {
    "staging": [
      {"name": "customer_id", "type": "IntegerType", "nullable": false},
      {"name": "first_name", "type": "StringType", "nullable": true},
      {"name": "batch_date", "type": "StringType", "nullable": false}
    ],
    "target": [
      {"name": "customer_id", "type": "IntegerType", "nullable": false},
      {"name": "first_name", "type": "StringType", "nullable": true},
      {"name": "batch_date", "type": "StringType", "nullable": false}
    ]
  }
}
```

---

## 🔍 Validation Checks

### Source → Staging Validations

| Check | Description | Failure Condition |
|-------|-------------|-------------------|
| **schemachk** | Validates schema structure | Missing fields, type mismatches, nullable differences |
| **count_val** | Record count comparison | Source count ≠ Staging count |
| **null_val** | Null/blank detection | Nulls found in non-nullable columns |
| **primary_key_val** | Duplicate PK detection | Duplicate primary keys found |
| **data_completeness** | Record loss check | Records in source missing in staging |
| **data_consistency** | Value-level comparison | Data values don't match between source and staging |

### Staging → Target Validations

| Check | Description | SCD Type |
|-------|-------------|----------|
| **schemachk** | Schema validation | All |
| **count_val_latest_batch** | Batch-specific count | Type 1 |
| **scd_type2_record_count** | Historical record count | Type 2 |
| **null_val** | Null detection | All |
| **primary_key_val** | Duplicate check | All |
| **scd_type2_active_flag** | Active flag validation | Type 2 |
| **scd_type2_history** | History preservation | Type 2 |
| **referential_integrity** | FK validation | All (if lookup configured) |
| **transformation_rules** | Business rule validation | All |

---

## 📊 Sample Output

### Successful Validation
```
######################################################################
  ETL VALIDATION FOR: CUSTOMER_DATA
  SCD Type: TYPE1
######################################################################

🔍 Starting Source → Staging Validation...

======================================================================
                 FINAL VALIDATION SUMMARY
======================================================================
FILE PROCESSED      : CUSTOMER_DATA
VALIDATION MODE     : both
----------------------------------------------------------------------

SOURCE → STAGING
----------------------------------------------------------------------
CHECK NAME                          STATUS     
----------------------------------------------------------------------
schemachk                           Success    ✔️
count_val                           Success    ✔️
null_val                            Success    ✔️
primary_key_val                     Success    ✔️
data_completeness                   Success    ✔️
data_consistency                    Success    ✔️

STAGING → TARGET
----------------------------------------------------------------------
CHECK NAME                          STATUS     
----------------------------------------------------------------------
schemachk                           Success    ✔️
count_val_latest_batch              Success    ✔️
null_val                            Success    ✔️
primary_key_val                     Success    ✔️
transformation_rules                Success    ✔️
----------------------------------------------------------------------
TOTAL CHECKS : 11 | PASSED: 11 | FAILED: 0
======================================================================

✅ Validation complete!
```

### Failed Validation with Details
```
CHECK NAME                          STATUS     
----------------------------------------------------------------------
primary_key_val                     Fail       ❌
    - Duplicates exist in the data

data_consistency                    Fail       ❌
    - Inconsistencies found: ['email: 2 mismatches', 'phone_number: 1 mismatches']
```

---

## 💼 Use Cases

### 1. **Daily ETL Pipeline Validation**
```bash
# Add to your ETL pipeline
spark-submit QA_Driver.py --file customer_data --mode both
```

### 2. **CI/CD Integration**
```yaml
# .github/workflows/etl-validation.yml
- name: Run ETL Validation
  run: python QA_Driver.py
```

### 3. **Data Migration Testing**
```bash
# Validate data migration from legacy to new system
python QA_Driver.py --file migrated_data --mode staging_to_target
```

### 4. **Data Quality Monitoring**
```bash
# Schedule daily validation checks
crontab -e
0 6 * * * /path/to/python QA_Driver.py >> /logs/etl_validation.log
```

---

## 🚀 Future Enhancements

### Planned Features
- [ ] **Unit Tests** - pytest framework with 80%+ coverage
- [ ] **HTML Reports** - Interactive dashboards with charts
- [ ] **Performance Metrics** - Execution time tracking per check
- [ ] **Parallel Execution** - Validate multiple files concurrently
- [ ] **Data Profiling** - Min/Max/Avg statistics
- [ ] **Parquet Support** - Handle additional file formats
- [ ] **Email Notifications** - Alert on validation failures
- [ ] **Historical Trending** - Track validation results over time
- [ ] **Web UI** - Real-time monitoring dashboard
- [ ] **ML-based Anomaly Detection** - Intelligent data quality checks

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📧 Contact

**Your Name** - https://www.linkedin.com/in/veena-gajjada-19831337/  

Project Link: https://github.dev/veenareddy514/ETL_Test_Automation
---


---

## 🙏 Acknowledgments

- Built with PySpark for scalable data processing
- Inspired by enterprise data quality frameworks
- Designed for real-world ETL validation scenarios

---

**⭐ If you find this project useful, please star the repository!**