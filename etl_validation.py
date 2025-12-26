"""
ETL Validation Module
Comprehensive data quality validation for ETL pipelines
Supports SCD Type 1 and Type 2 validations
"""

from pyspark.sql.functions import *
from pyspark.sql.types import StringType


class Etl_Val:
    def __init__(self, ss, file_name, expected_schema, source_df, target_df, primary_key, 
                 non_nullable_columns, validation_stage, scd_type, active_flag_column=None, 
                 lookup_details=None):
        """
        ETL Validation Class
        
        Parameters:
        - ss: SparkSession
        - file_name: Name of the dataset being validated
        - expected_schema: Expected schema structure
        - source_df: Source DataFrame
        - target_df: Target DataFrame
        - primary_key: List of primary key columns
        - non_nullable_columns: Columns that should not have nulls
        - validation_stage: "source_to_staging" or "staging_to_target"
        - scd_type: "type1" or "type2"
        - active_flag_column: Column name for SCD Type 2 active flag
        - lookup_details: Dictionary with lookup table details for referential integrity
        """
        self.ss = ss
        self.primary_key = primary_key
        self.non_nullable_columns = non_nullable_columns
        self.file_name = file_name
        self.expected_schema = expected_schema
        self.source_df = source_df
        self.target_df = target_df
        self.lookup_details = lookup_details
        self.validation_stage = validation_stage
        self.scd_type = scd_type
        self.active_flag_column = active_flag_column
        
        # Internal tracking lists
        self.trgt_fields_dt = []
        self.expected_fields_dt = []
        self.trgt_fields_nullable = []
        self.expected_fields_nullable = []
        self.ex_fields_not_in_tgt = []
        self.extra_fields_in_tgt = []
        self.msg_list = []
        self.validated_list = []
        
        # Run validations based on stage
        self.run_all_validations()
        
    def run_all_validations(self):
        """Run validations based on stage"""
        if self.validation_stage == "source_to_staging":
            self._run_source_to_staging_validations()
        elif self.validation_stage == "staging_to_target":
            self._run_staging_to_target_validations()

    def _run_source_to_staging_validations(self):
        """Validations for Source → Staging"""        

        # 1. Schema check
        success, msg = self.schemachk()
        self.validated_list.append({"check": "schemachk", "status": success, "details": msg})

        # 2. Count check
        success, src_count, trgt_count = self.count_val()
        self.validated_list.append({"check": "count_val", "status": success, "src_count": src_count, "trgt_count": trgt_count})

        # 3. Null check
        success, msg = self.null_val()
        self.validated_list.append({"check": "null_val", "status": success, "details": msg})

        # 4. Primary key check
        success, msg = self.primary_key_val()
        self.validated_list.append({"check": "primary_key_val", "status": success, "details": msg})

        # 5. Data completeness (no record loss)
        success, msg = self.data_completeness_check()
        self.validated_list.append({
            "check": "data_completeness", 
            "status": success, 
            "details": msg
        })

        # 6. Data consistency check (comparing key columns)
        success, msg = self.data_consistency_check()
        self.validated_list.append({
            "check": "data_consistency", 
            "status": success, 
            "details": msg
        })

    def _run_staging_to_target_validations(self):
        """Validations for Staging → Target"""
        
        # 1. Schema check
        success, msg = self.schemachk()
        self.validated_list.append({"check": "schemachk", "status": success, "details": msg})

        # 2. Count check (depends on SCD type)
        if self.scd_type == "type1":
            success, msg = self.count_val_by_latest_batch()
            self.validated_list.append({"check": "count_val_latest_batch", "status": success, "details": msg})
        elif self.scd_type == "type2":
            success, msg = self.scd_type2_count_check()
            self.validated_list.append({
                "check": "scd_type2_record_count", 
                "status": success, 
                "details": msg
            })    

        # 3. Null check
        success, msg = self.null_val()
        self.validated_list.append({"check": "null_val", "status": success, "details": msg})

        # 4. Primary key check
        success, msg = self.primary_key_val()
        self.validated_list.append({"check": "primary_key_val", "status": success, "details": msg})

        # 5. SCD Type 2 specific checks
        if self.scd_type == "type2":
            success, msg = self.scd_type2_active_flag_check()
            self.validated_list.append({
                "check": "scd_type2_active_flag", 
                "status": success, 
                "details": msg
            })
            
            success, msg = self.scd_type2_history_check()
            self.validated_list.append({
                "check": "scd_type2_history", 
                "status": success, 
                "details": msg
            })

        # 6. Referential integrity check (if lookup exists)
        if self.lookup_details:
            success, msg = self.referential_integrity_val()
            self.validated_list.append({
                "check": "referential_integrity", 
                "status": success, 
                "details": msg
            })
        
        # 7. Data transformation validation
        success, msg = self.transformation_validation()
        self.validated_list.append({
            "check": "transformation_rules", 
            "status": success, 
            "details": msg
        })

    def schemachk(self):
        """Validate schema structure, data types, and nullable constraints"""
        trgt_schema = self.target_df.schema
        df_fields = {field.name: field for field in trgt_schema.fields}
        ex_fields = {field.name: field for field in self.expected_schema.fields}
        
        for field_name in ex_fields:
            if field_name not in df_fields:
                self.msg_list.append(f"Missing field in target data frame: '{field_name}'")
                self.ex_fields_not_in_tgt.append(field_name)
                continue
            
            df_field = df_fields[field_name]
            ex_field = ex_fields[field_name]
          
            if df_field.dataType != ex_field.dataType:
                self.trgt_fields_dt.append(df_field)
                self.expected_fields_dt.append(ex_field)
                self.msg_list.append(
                    f"Type mismatch for field '{field_name}': target has '{df_field.dataType}', expected '{ex_field.dataType}'"
                )
            
            if df_field.nullable != ex_field.nullable:
                self.trgt_fields_nullable.append(df_field)
                self.expected_fields_nullable.append(ex_field)
                self.msg_list.append(
                    f"Nullable mismatch for field '{field_name}': target has '{df_field.nullable}', expected '{ex_field.nullable}'"
                )
        
        for field_name in df_fields:
            if field_name not in ex_fields:
                self.msg_list.append(f"Extra field in target DataFrame not expected: '{field_name}'")
                self.extra_fields_in_tgt.append(field_name)

        if not self.msg_list:
            self.msg_list.append("Schema is valid")
            return ('Success', self.msg_list)
        else:
            return ('Fail', self.msg_list)

    def count_val(self):
        """Basic count validation - source count should equal target count"""
        src_count = self.source_df.count()
        trgt_count = self.target_df.count()
        if src_count == trgt_count:
            return ('Success', src_count, trgt_count)
        else:
            return ('Fail', src_count, trgt_count)

    def null_val(self):
        """Check for nulls or blanks in non-nullable columns"""
        null_report = {}

        for field in self.non_nullable_columns:
            # Get the field data type in target DataFrame
            dtype = [f.dataType for f in self.target_df.schema.fields if f.name == field][0]
            
            if isinstance(dtype, StringType):
                # For strings: check None/null/empty/blank
                null_count = self.target_df.filter(
                    (col(field).isNull()) | (trim(col(field)) == "") | (col(field) == None)
                ).count()
            else:
                # For numeric/non-string: check only null/None
                null_count = self.target_df.filter(col(field).isNull() | (col(field) == None)).count()
            
            if null_count > 0:
                null_report[field] = null_count

        if not null_report:
            return ('Success', 'No Nulls or blank in the non-nullable columns')
        else:
            return ("Fail", f"Nulls or blank found in columns: {null_report}")

    def primary_key_val(self):
        """Check for duplicate primary keys"""
        pk_str = ",".join(self.primary_key)
        self.target_df.createOrReplaceTempView("trgt_tbl")
        result_df = self.ss.sql(f"""
            SELECT {pk_str}, count(*) AS total_count
            FROM trgt_tbl 
            GROUP BY {pk_str} 
            HAVING count(*) > 1
        """)
        
        record_count = result_df.count()
        if record_count == 0:
            return ('Success', 'No Duplicates in the data')
        else:
            return ('Fail', "Duplicates exist in the data")

    def count_val_by_latest_batch(self):
        """
        Count validation for staging→target based on the latest batch_date.
        Used for SCD Type 1 where we compare only the latest batch.
        """
        # Validate batch_date column exists
        if "batch_date" not in self.source_df.columns or "batch_date" not in self.target_df.columns:
            return ("Fail", "batch_date column missing in source or target")

        # Get MAX batch date (more efficient and reliable)
        max_batch_row = self.source_df.agg({"batch_date": "max"}).collect()
        
        if not max_batch_row or max_batch_row[0][0] is None:
            return ("Fail", "No batch_date found in staging table")

        batch_date = max_batch_row[0][0]

        # Count for that specific batch
        src_count = self.source_df.filter(col("batch_date") == batch_date).count()
        trgt_count = self.target_df.filter(col("batch_date") == batch_date).count()

        if src_count == trgt_count:
            return ("Success", f"Batch {batch_date}: Count matched → {src_count}")
        else:
            return ("Fail", f"Batch {batch_date}: Source={src_count}, Target={trgt_count}")

    def referential_integrity_val(self):
        """Validate foreign key relationships using lookup tables"""
        target_col = self.lookup_details.get("target_column")
        lookup_df = self.lookup_details.get("lookup_table")
        lookup_col = self.lookup_details.get("lookup_column")
        
        # Find records in target that don't exist in lookup
        missing_df = self.target_df.join(
            lookup_df, 
            self.target_df[target_col] == lookup_df[lookup_col], 
            "left_anti"
        )
        missing_count = missing_df.count()
        
        if missing_count == 0:
            return ("Success", f"All {target_col} values exist in lookup table")
        
        missing_values = [row[target_col] for row in missing_df.limit(10).collect()]
        return ("Fail", f"{missing_count} invalid {target_col} values: {missing_values}")

    # ============ SOURCE TO STAGING SPECIFIC ============
    
    def data_completeness_check(self):
        """Ensure no records are lost from source to staging"""
        src_keys = self.source_df.select(*self.primary_key).distinct()
        stg_keys = self.target_df.select(*self.primary_key).distinct()
        
        # Find keys in source but not in staging
        missing_in_stg = src_keys.join(stg_keys, self.primary_key, "left_anti")
        missing_count = missing_in_stg.count()
        
        if missing_count == 0:
            return ('Success', 'All source records present in staging')
        else:
            missing_keys = [row.asDict() for row in missing_in_stg.limit(10).collect()]
            return ('Fail', f'{missing_count} source records missing in staging: {missing_keys}')
    
    def data_consistency_check(self):
        """Compare key business columns between source and staging"""
        # Join on primary key and compare values
        join_condition = [self.source_df[k] == self.target_df[k] for k in self.primary_key]
        
        joined_df = self.source_df.alias("src").join(
            self.target_df.alias("stg"),
            join_condition,
            "inner"
        )
        
        inconsistencies = []
        # Check each column for consistency
        for field in self.source_df.schema.fields:
            col_name = field.name
            if col_name in self.primary_key:
                continue
            
            if col_name in [f.name for f in self.target_df.schema.fields]:
                mismatch_count = joined_df.filter(
                    (col(f"src.{col_name}").isNull() & col(f"stg.{col_name}").isNotNull()) |
                    (col(f"src.{col_name}").isNotNull() & col(f"stg.{col_name}").isNull()) |
                    ((col(f"src.{col_name}").isNotNull() & col(f"stg.{col_name}").isNotNull()) & 
                     (col(f"src.{col_name}") != col(f"stg.{col_name}")))
                ).count()
                
                if mismatch_count > 0:
                    inconsistencies.append(f"{col_name}: {mismatch_count} mismatches")
        
        if not inconsistencies:
            return ('Success', 'Data is consistent between source and staging')
        else:
            return ('Fail', f'Inconsistencies found: {inconsistencies}')
    
    # ============ STAGING TO TARGET SPECIFIC ============
    
    def scd_type2_count_check(self):
        """Count check for SCD Type 2 - target can have more records due to history"""
        src_count = self.source_df.count()
        trgt_count = self.target_df.count()
        
        # For Type 2, target should have >= source (historical records)
        if trgt_count >= src_count:
            return ('Success', f'Target count ({trgt_count}) >= Staging count ({src_count})')
        else:
            return ('Fail', f'Target count ({trgt_count}) < Staging count ({src_count})')
    
    def scd_type2_active_flag_check(self):
        """Validate active flag for SCD Type 2 - each business key should have exactly one active record"""
        if not self.active_flag_column:
            return ('Fail', 'No active flag column specified for SCD Type 2')
        
        # Check each business key has exactly one active record
        pk_str = ",".join(self.primary_key)
        self.target_df.createOrReplaceTempView("scd_check")
        
        # Handle both string ("Y") and boolean (True) types
        result_df = self.ss.sql(f"""
            SELECT {pk_str}, 
                   SUM(CASE 
                       WHEN {self.active_flag_column} IN ('Y', 'TRUE', '1', TRUE) THEN 1 
                       ELSE 0 
                   END) as active_count
            FROM scd_check
            GROUP BY {pk_str}
            HAVING active_count != 1
        """)
        
        invalid_count = result_df.count()
        if invalid_count == 0:
            return ('Success', 'Each business key has exactly one active record')
        else:
            invalid_keys = [row.asDict() for row in result_df.limit(5).collect()]
            return ('Fail', f'{invalid_count} business keys have invalid active flag: {invalid_keys}')
    
    def scd_type2_history_check(self):
        """Validate historical records are preserved"""
        if not self.active_flag_column:
            return ('Success', 'No SCD Type 2 validation needed')
        
        # Count inactive records (historical records should exist)
        inactive_count = self.target_df.filter(
            (col(self.active_flag_column) == 'N') | 
            (col(self.active_flag_column) == False) |
            (col(self.active_flag_column) == 'FALSE') |
            (col(self.active_flag_column) == '0')
        ).count()
        
        return ('Success', f'Found {inactive_count} historical records preserved')
    
    def transformation_validation(self):
        """Validate business transformation rules"""
        # Add custom transformation checks based on business rules
        
        # Example: Check if all amounts are positive
        if "amount" in [f.name for f in self.target_df.schema.fields]:
            negative_amounts = self.target_df.filter(col("amount") < 0).count()
            if negative_amounts > 0:
                return ('Fail', f'Found {negative_amounts} records with negative amounts')
        
        return ('Success', 'Transformation validation passed')