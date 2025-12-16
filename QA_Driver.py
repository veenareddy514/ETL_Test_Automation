from load_data import *
from etl_validation import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Main function to drive process
def main(file_name,src_file_path,trgt_file_path,expected_schema,primary_key,non_nullable_columns):
    ss=SparkSession.builder.appName("ETL Validation").getOrCreate()
    # Load source and target data
    loader=LoadData(ss,file_name,src_file_path,trgt_file_path)
    data_load_status = loader.status
    source_df = loader.src_df
    target_df = loader.trgt_df

    # Run schema check on target DataFrame
    #schemachk=Etl_Val(ss,file_name,expected_schema,target_df)
     # Extract schema check results
    #msg=schemachk.msg_list
    #ex_fields_not_in_tgt=schemachk.ex_fields_not_in_tgt
    #extra_fields_in_tgt=schemachk.extra_fields_in_tgt
    #trgt_fields_dt=schemachk.trgt_fields_dt 
    #expected_fields_dt= schemachk.expected_fields_dt 
    #trgt_fields_nullable= schemachk.trgt_fields_nullable 
    #expected_fields_nullable=schemachk.expected_fields_nullable
    # Print schema check summary
    #print("=== Schema Check Messages ===")
    ##   print(m)
    #success_bool,src_count,trgt_count
    etl_val=Etl_Val(ss,file_name,expected_schema,source_df,target_df,primary_key,non_nullable_columns) 

    etl_checks=etl_val.validated_list 
    print(type(etl_checks))
    print(etl_checks) 




  


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
            "primary_key": "customer_id",
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
            "primary_key": "order_id",
            "non_nullable_columns":['order_id','customer_id']
        }
    }

    if file_name in files_info:
        info = files_info[file_name]
        return file_name, info["src_file_path"], info["trgt_file_path"], info["expected_schema"], info["primary_key"],info["non_nullable_columns"]
    else:
        raise ValueError(f"File name '{file_name}' not recognized. Available options: {list(files_info.keys())}")





if __name__=='__main__':
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

    # Get file details
    file_name,src_file_path,trgt_file_path,expected_schema,primary_key,non_nullable_columns=file_details(file_name)
    # Run main ETL validation
    main(file_name,src_file_path,trgt_file_path,expected_schema,primary_key,non_nullable_columns)
