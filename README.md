
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
├── etl_validation.py          # Validation logic  to validate the data  of ETL Pipeline
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

- Correctness of data extraction
- Accuracy of transformations
- Integrity of loaded data

## Technologies Used

- Python 3.x


## Author

Veena Gajjada
