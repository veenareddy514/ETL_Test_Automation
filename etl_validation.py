from  pyspark.sql.functions import *
from pyspark.sql.types import StringType
class Etl_Val:
    def __init__(self,ss,file_name,expected_schema,source_df,target_df,primary_key,non_nullable_columns, lookup_details=None):
        self.ss=ss
        self.primary_key=primary_key
        self.non_nullable_columns=non_nullable_columns
        self.file_name=file_name
        self.expected_schema=expected_schema
        self.source_df=source_df
        self.target_df=target_df
        self.trgt_fields_dt=[]
        self.expected_fields_dt=[]
        self.trgt_fields_nullable=[]
        self.expected_fields_nullable=[]
        self.ex_fields_not_in_tgt=[]
        self.extra_fields_in_tgt=[]
        self.msg_list = []
        self.validated_list=[]
        self.run_all_validations()
        #self.msg_list,self.ex_fields_not_in_tgt,self.extra_fields_in_tgt,self.trgt_fields_dt, self.expected_fields_dt, self.trgt_fields_nullable, self.expected_fields_nullable=self.schemachk()
        
    def run_all_validations(self):
        # Schema check
        success, msg = self.schemachk()
        self.validated_list.append({"check": "schemachk", "status": success, "details": msg})

        # Count check
        success, src_count, trgt_count = self.count_val()
        self.validated_list.append({"check": "count_val", "status": success, "src_count": src_count, "trgt_count": trgt_count})

        # Null check
        success, msg = self.null_val()
        self.validated_list.append({"check": "null_val", "status": success, "details": msg})

        # Primary key check
        success, msg = self.primary_key_val()
        self.validated_list.append({"check": "primary_key_val", "status": success, "details": msg})

        # Referential integrity check (optional)
        if self.lookup_details:
            success, msg = self.referential_integrity_val()
            self.validated_list.append({"check": "referential_integrity", "status": success, "details": msg})
    
    def schemachk(self):
        
        trgt_schema=self.target_df.schema
        df_fields={field.name:field for field in trgt_schema.fields}
        ex_fields={field.name:field for field in self.expected_schema.fields}
        
        for field_name in ex_fields:
            if field_name not in df_fields:
                self.msg_list.append(f"Missing field in target data frame'{field_name}'")
                self.ex_fields_not_in_tgt.append(field_name)
                continue
            
            df_field=df_fields[field_name]
            ex_field=ex_fields[field_name]
          
            if (df_field.dataType!=ex_field.dataType) :
                self.trgt_fields_dt.append(df_field)
                self.expected_fields_dt.append(ex_field)

                self.msg_list.append(
                f"Type mismatch for field '{field_name}': target has '{df_field.dataType}', expected '{ex_field.dataType}'")
            if df_field.nullable!=ex_field.nullable:
                self.trgt_fields_nullable.append(df_field)
                self.expected_fields_nullable.append(ex_field)
                self.msg_list.append(
                f"Nullable mismatch for field '{field_name}': target has '{df_field.nullable}', expected '{ex_field.nullable}'" )
        for field_name in df_fields:
            if field_name not in ex_fields:
                self.msg_list.append(f"Extra field in target DataFrame not expected: '{field_name}'")
                self.extra_fields_in_tgt.append(field_name)



        if not self.msg_list:
                self.msg_list.append("Schema is valid")
                return ('Success',self.msg_list)
        else:
                return ('Fail',self.msg_list)
    def count_val(self):
        src_count=self.source_df.count()
        trgt_count=self.target_df.count()
        if src_count==trgt_count:
            return ('Success',src_count,trgt_count)
        else: 
            return ('Fail',src_count,trgt_count)   

    def null_val(self):
        null_report={}
        self.target_df.show()

        for field in self.non_nullable_columns:
            print(field)
             # Get the field data type in target DataFrame
            dtype = [f.dataType for f in self.target_df.schema.fields if f.name == field][0]
            print(dtype)

            if isinstance(dtype, StringType):
            # For strings: check None/null/empty/blank
                null_count = self.target_df.filter(
                (col(field).isNull()) | (trim(col(field)) == "") | (col(field) == None)).count()
            else:
            # For numeric/non-string: check only null/None
                null_count = self.target_df.filter(col(field).isNull() | (col(field) == None)).count()
            if null_count > 0:
                null_report[field] = null_count    


        if not null_report:
                return ('Success','No Nulls or blank in the Nullable columns')   
        else:
                return ("Fail",f"Nulls or blank found in columns:{null_report}")   

    def primary_key_val(self):
        pk_str=",".join(self.primary_key)
        self.target_df.createOrReplaceTempView("trgt_tbl")
        result_df=self.ss.sql(f"""SELECT {pk_str},
                                   count(*) AS total_count
                                   FROM trgt_tbl GROUP BY {pk_str} having count(*)>1""")
        
        record_count=result_df.count()
        if record_count<0:
            return('Success','No Duplicates in the data')
        else:
            return('Fail',"Duplicates exist in the data")


    def referential_integrity_val(self):
        target_col = self.lookup_details.get("target_column")
        lookup_df = self.lookup_details.get("lookup_table")
        lookup_col = self.lookup_details.get("lookup_column")
        missing_df = self.target_df.join(lookup_df, self.target_df[target_col] == lookup_df[lookup_col], "left_anti")
        missing_count = missing_df.count()
        if missing_count == 0:
            return ("Success", f"All {target_col} values exist in lookup table")
        missing_values = [row[target_col] for row in missing_df.collect()]
        return ("Fail", f"{missing_count} invalid {target_col} values: {missing_values}")       


    
            
        




