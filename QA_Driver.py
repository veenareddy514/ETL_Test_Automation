"""
ETL Validation Framework - Main Driver
Orchestrates the validation process for ETL pipelines
Supports SCD Type 1 and Type 2 validations
"""

import json
import os
import builtins
from load_data import LoadData
from etl_validation import Etl_Val
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, trim

# Validate Java installation
java_home = os.environ.get("JAVA_HOME", "")
if not java_home or "java-17" not in java_home:
    raise EnvironmentError(
        "Spark requires Java 11 or 17. Please set JAVA_HOME correctly."
    )


def print_final_summary(file_name, validation_mode, results_list):
    """
    Print comprehensive validation summary with pass/fail indicators
    
    Parameters:
    - file_name: Name of the dataset validated
    - validation_mode: Validation mode used
    - results_list: List of validation results
    """
    print("\n" + "="*70)
    print(f"                 FINAL VALIDATION SUMMARY")
    print("="*70)
    print(f"FILE PROCESSED      : {file_name.upper()}")
    print(f"VALIDATION MODE     : {validation_mode}")
    print("-"*70)

    if validation_mode == "both":
        mid = len(results_list)//2
        sections = [("SOURCE → STAGING", results_list[:mid]), 
                    ("STAGING → TARGET", results_list[mid:])]
    else:
        sections = [("RESULTS", results_list)]

    total_passed = 0
    total_checks = 0

    for section_name, section_results in sections:
        print(f"\n{section_name}")
        print("-"*70)
        print("{:<35} {:<10} {}".format("CHECK NAME", "STATUS", ""))
        print("-"*70)

        for res in section_results:
            status = res.get("status", "N/A")
            icon = "✔️" if status == "Success" else "❌"
            print("{:<35} {:<10} {}".format(res["check"], status, icon))

        passed = builtins.sum(1 for r in section_results if r["status"] == "Success")
        total_passed += passed
        total_checks += len(section_results)

    print("-"*70)
    failed = total_checks - total_passed
    print(f"TOTAL CHECKS : {total_checks} | PASSED: {total_passed} | FAILED: {failed}")
    print("="*70 + "\n")


def validate_source_to_staging(ss, file_name, staging_schema, source_df, stage_df, 
                                primary_key, non_nullable_columns):
    """
    Validate Source → Staging transformation
    
    Parameters:
    - ss: SparkSession
    - file_name: Dataset name
    - staging_schema: Expected staging schema
    - source_df: Source DataFrame
    - stage_df: Staging DataFrame
    - primary_key: List of primary key columns
    - non_nullable_columns: Columns that cannot be null
    
    Returns:
    - List of validation results
    """
    etl_val = Etl_Val(
        ss=ss,
        file_name=file_name,
        expected_schema=staging_schema,
        source_df=source_df,
        target_df=stage_df,
        primary_key=primary_key,
        non_nullable_columns=non_nullable_columns,
        lookup_details=None,
        validation_stage="source_to_staging",
        scd_type=None
    )
    
    return etl_val.validated_list


def validate_staging_to_target(ss, file_name, target_schema, stage_df, target_df, 
                                primary_key, non_nullable_columns, lookup_details, 
                                scd_type, active_flag_column=None):
    """
    Validate Staging → Target transformation
    
    Parameters:
    - ss: SparkSession
    - file_name: Dataset name
    - target_schema: Expected target schema
    - stage_df: Staging DataFrame
    - target_df: Target DataFrame
    - primary_key: List of primary key columns
    - non_nullable_columns: Columns that cannot be null
    - lookup_details: Lookup table details for referential integrity
    - scd_type: SCD type (type1 or type2)
    - active_flag_column: Column name for active flag (SCD Type 2)
    
    Returns:
    - List of validation results
    """
    etl_val = Etl_Val(
        ss=ss,
        file_name=file_name,
        expected_schema=target_schema,
        source_df=stage_df,
        target_df=target_df,
        primary_key=primary_key,
        non_nullable_columns=non_nullable_columns,
        lookup_details=lookup_details,
        validation_stage="staging_to_target",
        scd_type=scd_type,
        active_flag_column=active_flag_column
    )
    
    return etl_val.validated_list


def parse_schema(schema_json):
    """
    Parse schema from JSON to PySpark StructType
    
    Parameters:
    - schema_json: List of field definitions from JSON
    
    Returns:
    - StructType: PySpark schema
    """
    type_map = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "FloatType": FloatType(),
        "BooleanType": BooleanType()
    }
    fields = [
        StructField(f["name"], type_map[f["type"]], nullable=f.get("nullable", True)) 
        for f in schema_json
    ]
    return StructType(fields)


def main(file_name, file_config, schema_config, ss, validation_mode):
    """
    Main validation driver
    
    Parameters:
    - file_name: Name of dataset to validate
    - file_config: File configuration (paths, keys, etc.)
    - schema_config: Schema definitions
    - ss: SparkSession
    - validation_mode: 'source_to_staging', 'staging_to_target', or 'both'
    """
    print(f"\n{'#'*70}")
    print(f"  ETL VALIDATION FOR: {file_name.upper()}")
    print(f"  SCD Type: {file_config[file_name].get('scd_type', 'type1').upper()}")
    print(f"{'#'*70}")

    # Extract file configuration
    file_info = file_config[file_name]
    primary_key = file_info["primary_key"]
    scd_type = file_info["scd_type"]
    
    # Get active flag column for SCD Type 2
    active_flag_column = file_info.get("active_flag_column", None)
    
    # Handle non_nullable_columns (supports both formats)
    if "non_nullable_columns" in file_info:
        # Simple list format
        non_nullable_columns = file_info["non_nullable_columns"]
    elif "non_nullable" in file_info:
        # Stage-specific format (dict)
        if validation_mode in ["source_to_staging", "both"]:
            non_nullable_columns = file_info["non_nullable"].get("source_to_staging", [])
        elif validation_mode in ["staging_to_target"]:
            non_nullable_columns = file_info["non_nullable"].get("staging_to_target", [])
    else:
        non_nullable_columns = []

    # Extract file paths
    src_file_path = file_info["src_file_path"]
    stg_file_path = file_info["stg_file_path"]
    trgt_file_path = file_info["trgt_file_path"]
    
    # Parse expected schemas
    schema_info = schema_config[file_name]
    staging_schema = parse_schema(schema_info["staging"])
    target_schema = parse_schema(schema_info["target"])
   
    # Load data
    loader = LoadData(ss, file_name, src_file_path, stg_file_path, trgt_file_path, validation_mode)
    data_load_status = loader.status
    
    if not data_load_status:
        print("❌ Data loading failed. Exiting validation.")
        return
    
    source_df = loader.src_df
    stage_df = loader.stg_df
    target_df = loader.trgt_df

    # Lookup details if present (for referential integrity)
    lookup_details = None
    if "lookup" in file_info:
        lookup_info = file_info["lookup"]
        lookup_file = lookup_info["lookup_file"]
        lookup_src = file_config[lookup_file]["src_file_path"]
        lookup_stg = file_config[lookup_file]["stg_file_path"]
        lookup_trgt = file_config[lookup_file]["trgt_file_path"]

        lookup_df = LoadData(ss, lookup_file, lookup_src, lookup_stg, lookup_trgt, "both").trgt_df
        lookup_details = {
            "lookup_table": lookup_df,
            "lookup_column": lookup_info["lookup_column"],
            "target_column": lookup_info["target_column"]
        }

    # Run validations based on mode
    if validation_mode in ["source_to_staging", "both"]:
        print(f"\n🔍 Starting Source → Staging Validation...")
        src_to_stg_checks = validate_source_to_staging(
            ss, file_name, staging_schema, source_df, stage_df, 
            primary_key, non_nullable_columns
        )

    if validation_mode in ["staging_to_target", "both"]:
        print(f"\n🔍 Starting Staging → Target Validation...")
        
        # Update non_nullable_columns for staging_to_target if needed
        if "non_nullable" in file_info and isinstance(file_info["non_nullable"], dict):
            non_nullable_columns = file_info["non_nullable"].get("staging_to_target", [])
        
        stg_to_trgt_checks = validate_staging_to_target(
            ss, file_name, target_schema, stage_df, target_df, 
            primary_key, non_nullable_columns, lookup_details, 
            scd_type, active_flag_column
        )

    # Compile results
    if validation_mode == "source_to_staging":
        results = src_to_stg_checks
    elif validation_mode == "staging_to_target":
        results = stg_to_trgt_checks
    else:
        results = src_to_stg_checks + stg_to_trgt_checks

    # Print final summary
    print_final_summary(file_name, validation_mode, results)


if __name__ == '__main__':
    # Initialize Spark Session
    ss = SparkSession.builder \
        .appName("ETL Validation") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/tmp") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()
    
    # Load configuration files
    with open("file_config.json") as f:
        file_config = json.load(f)

    with open("schema_config.json") as f:
        schema_config = json.load(f)

    # Present available files to user
    available_files = ["customer_data", "orders_data"]
    print("\n" + "="*70)
    print("Available files for ETL validation:")
    print("="*70)
    for idx, f in enumerate(available_files, 1):
        scd_type = file_config[f].get("scd_type", "type1")
        print(f"{idx}. {f:<20} (SCD {scd_type.upper()})")

    # Take user input for file selection
    print("="*70)
    selected_idx = int(input("Enter the number corresponding to the file you want to validate: "))

    if 1 <= selected_idx <= len(available_files):
        file_name = available_files[selected_idx - 1]
    else:
        raise ValueError("Invalid selection.")

    # Ask for validation mode
    print("\n" + "-"*70)
    print("Validation Mode Options:")
    print("-"*70)
    print("1. Source → Staging only")
    print("2. Staging → Target only")
    print("3. Both (Complete Pipeline)")
    print("-"*70)
    
    mode_idx = int(input("\nEnter validation mode (1-3): "))
    mode_map = {
        1: "source_to_staging",
        2: "staging_to_target",
        3: "both"
    }
    validation_mode = mode_map.get(mode_idx, "both")

    # Run Validation
    main(file_name, file_config, schema_config, ss, validation_mode)
    
    print("\n✅ Validation complete!")
    
    # Stop Spark session
    ss.stop()