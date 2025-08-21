
# ETL Test Automation Sample Project

This sample project demonstrates automated testing of ETL pipelines. It validates data extraction, transformation, and loading using pyspark and python

## Structure

## Project Structure

ETL_TEST_AUTOMATION/

├── customer_src.csv           # Source customer data

├── customer_trgt.csv          # Target customer data

├── orders_src.csv             # Source orders data

├── orders_trgt.csv            # Target orders data

│
├── etl_validation.py          # Validation logic (schema, counts, null checks)

├── file_creation.py           # Script to generate/create test data files

├── load_data.py               # Script to load data into source and target dataframes

├── QA_Driver.py               # Main driver script to execute ETL validation
│

├── README.md                  # Project documentation

└── requirements.txt           # (optional) Python dependencies


## How to Run

1. Clone the repository
2. Install requirements:  
   `pip install -r requirements.txt`
3. Run tests:  
    python3 QA_Driver.py

## What is Tested?

✅ Schema Validation
Ensures source/target fields match in name, datatype, and nullability.

✅ Record Count Validation
Checks that the number of records in source and target match.

✅ Null / Blank Value Validation
Verifies that non-nullable fields do not contain nulls, blanks, or empty values.

## Technologies Used

- Python 3.x
- PySpark


## Author

Veena Gajjada
