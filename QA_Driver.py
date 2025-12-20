import json
from load_data import *
from etl_validation import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import os

java_home = os.environ.get("JAVA_HOME", "")
if not java_home or "java-17" not in java_home:
    raise EnvironmentError(
        "Spark requires Java 11 or 17. Please set JAVA_HOME correctly."
    )


# Function to parse schema from JSON to StructType
def parse_schema(schema_json):
    type_map = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "FloatType": FloatType(),
        "BooleanType": BooleanType()
    }
    fields = [StructField(f["name"], type_map[f["type"]], nullable=f.get("nullable", True)) for f in schema_json]
    return StructType(fields)

#Main function to drive process
def main(file_name,config,ss):
    info = config[file_name]
    print(type(config))
    print(config)
    # Parse expected schema
    expected_schema = parse_schema(info["expected_schema"])
    primary_key = info["primary_key"]
    non_nullable_columns = info["non_nullable_columns"]
    src_file_path=info["src_file_path"]

    trgt_file_path=info["trgt_file_path"]

    # Load source and target data
    loader=LoadData(ss,file_name,src_file_path,trgt_file_path)
    data_load_status = loader.status
    source_df = loader.src_df
    target_df = loader.trgt_df

    # Lookup details if present
    lookup_details = None
    if "lookup" in info:
        lookup_info = info["lookup"]
        lookup_file = lookup_info["lookup_file"]
        lookup_df = LoadData(ss, lookup_file, config[lookup_file]["trgt_file_path"], config[lookup_file]["trgt_file_path"]).trgt_df
        lookup_details = {
            "lookup_table": lookup_df,
            "lookup_column": lookup_info["lookup_column"],
            "target_column": lookup_info["target_column"]
        }
    # ETL validation
    etl_val = Etl_Val(ss, file_name, expected_schema, source_df, target_df, primary_key, non_nullable_columns, lookup_details)    

    
    
    etl_checks=etl_val.validated_list 
    print("\n================ ETL VALIDATION SUMMARY ================\n")
    for res in etl_checks:
        check = res["check"]
        status = res["status"]

        
        icon = "✔" if status == "Success" else "✖"
        print(f"{icon} {check.upper():<25}: {status.upper()}")

        if "details" in res and res["details"]:
            if isinstance(res["details"], list):
                for msg in res["details"]:
                    print(f"    - {msg}")
            else:
                print(f"    - {res['details']}")

        if "src_count" in res:
            print(f"    - Source Count: {res['src_count']}")
            print(f"    - Target Count: {res['trgt_count']}")

        print()

    print("========================================================\n")



if __name__=='__main__':
    ss = SparkSession.builder \
    .appName("ETL Validation") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "/tmp") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()
    # Load configuration
    with open("file_config.json") as f:
        config = json.load(f)

    # Present available files to user
    available_files = ["customer_data", "orders_data"]  # Add other file names here
    print("Available files for ETL validation:")
    for idx, f in enumerate(available_files, 1):
        print(f"{idx}. {f}")

    # Take user input
    selected_idx = int(input("Enter the number corresponding to the file you want to validate: "))

    
    if 1 <= selected_idx <= len(available_files):
        file_name = available_files[selected_idx - 1]
    else:
        raise ValueError("Invalid selection.")

    main(file_name, config, ss)    

    # Get file details
    #file_name,src_file_path,trgt_file_path,expected_schema,primary_key,non_nullable_columns=file_details(file_name)
    #print(file_name,src_file_path,trgt_file_path,expected_schema,primary_key,non_nullable_columns)
    # Run main ETL validation
    #main(file_name,src_file_path,trgt_file_path,expected_schema,primary_key,non_nullable_columns)
