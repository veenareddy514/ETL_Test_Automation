import json
from load_data import *
from etl_validation import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


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
def main(file_name,src_file_path,trgt_file_path,expected_schema,primary_key,non_nullable_columns):
    ss=SparkSession.builder.appName("ETL Validation").getOrCreate()
    # Load source and target data
    loader=LoadData(ss,file_name,src_file_path,trgt_file_path)
    data_load_status = loader.status
    source_df = loader.src_df
    target_df = loader.trgt_df

    
    etl_val=Etl_Val(ss,file_name,expected_schema,source_df,target_df,primary_key,non_nullable_columns) 

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



# Define expected schema and paths (example placeholders)

# Function to return file details based on selected file
def file_details(file_name):
    
    files_info = {
        "customer_data": {
            "src_file_path": "/workspaces/ETL_Test_Automation/customer_src.csv",
            "trgt_file_path": "/workspaces/ETL_Test_Automation/customer_trgt.csv",
            "expected_schema": StructType([
                StructField("customer_id", IntegerType(), nullable=False),
                StructField("first_name", StringType(), nullable=True),
                StructField("last_name", StringType(), nullable=True),
                StructField("email", StringType(), nullable=True),
                StructField("phone_number", StringType(), nullable=True),
                StructField("address", StringType(), nullable=True),
                StructField("city", StringType(), nullable=True),
                StructField("state", StringType(), nullable=True),
                StructField("zip_code", IntegerType(), nullable=True)
            ]),
            "primary_key": ["customer_id"],
            "non_nullable_columns":['customer_id']
        },
        # Add more files here as needed
        "orders_data": {
            "src_file_path": "/workspaces/ETL_Test_Automation/orders_src.csv",
            "trgt_file_path": "/workspaces/ETL_Test_Automation/orders_trgt.csv",
            "expected_schema": StructType([
                StructField("order_id", IntegerType(), nullable=False),
                StructField("customer_id", IntegerType(), nullable=False),
                StructField("order_date", StringType(), nullable=True),
                StructField("amount", IntegerType(), nullable=True)
            ]),
            "primary_key": ["order_id"],
            "non_nullable_columns":['order_id','customer_id']
        }
    }
    print("End of File Details")
    if file_name in files_info:
        info = files_info[file_name]
        print("Captured Info") 
        return file_name, info["src_file_path"], info["trgt_file_path"], info["expected_schema"], info["primary_key"],info["non_nullable_columns"]
    else:
        raise ValueError(f"File name '{file_name}' not recognized. Available options: {list(files_info.keys())}")





if __name__=='__main__':
    ss = SparkSession.builder.appName("ETL Validation").getOrCreate()
    # Load configuration
    with open("etl_config.json") as f:
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
