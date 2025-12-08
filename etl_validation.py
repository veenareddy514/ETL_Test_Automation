from  pyspark.sql.functions import *
from pyspark.sql.types import StringType
class Etl_Val:
    def __init__(self,ss,file_name,expected_schema,source_df,target_df,non_nullable_columns):
        self.ss=ss
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
        #self.msg_list,self.ex_fields_not_in_tgt,self.extra_fields_in_tgt,self.trgt_fields_dt, self.expected_fields_dt, self.trgt_fields_nullable, self.expected_fields_nullable=self.schemachk()
        success_bool,msg_list=self.schemachk()
        self.validated_list.append(('schemachk',success_bool,msg_list))
        success_bool,src_count,trgt_count=self.count_val()
        self.validated_list.append(('count_val',success_bool,src_count,trgt_count))
        success_bool,msg=self.null_val()
        self.validated_list.append(('null_val', success_bool, msg))
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

    def primary_key(self,):
        src_count=self.source_df.count()
        trgt_count=self.target_df.count()
        if src_count==trgt_count:
            return ('Success',src_count,trgt_count)
        else: 
            return ('Fail',src_count,trgt_count) 




