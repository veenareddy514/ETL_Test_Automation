"""
Data Loading Module for ETL Validation Framework
Handles loading of CSV files into PySpark DataFrames
"""

import os
from pyspark.sql import DataFrame


class LoadData:
    def __init__(self, ss, file_name, src_file_path, stg_file_path, trgt_file_path, validation_mode="both"):
        """
        Initialize LoadData class
        
        Parameters:
        - ss: SparkSession
        - file_name: Name of the dataset
        - src_file_path: Path to source CSV
        - stg_file_path: Path to staging CSV
        - trgt_file_path: Path to target CSV
        - validation_mode: "source_to_staging", "staging_to_target", or "both"
        """
        self.ss = ss
        self.file_name = file_name
        self.src_file_path = src_file_path
        self.stg_file_path = stg_file_path
        self.trgt_file_path = trgt_file_path
        self.validation_mode = validation_mode
        self.status, self.src_df, self.stg_df, self.trgt_df = self.load_data()
    
    def load_data(self):
        """
        Load CSV files based on validation mode
        Only loads files that are needed for the specified validation
        """
        try:
            src_df = None
            stg_df = None
            trgt_df = None
            
            # Load files based on validation mode
            if self.validation_mode in ["source_to_staging", "both"]:
                src_df = self._load_csv(self.src_file_path, "source")
                stg_df = self._load_csv(self.stg_file_path, "staging")
            
            if self.validation_mode in ["staging_to_target", "both"]:
                if stg_df is None:  # Avoid loading twice if "both"
                    stg_df = self._load_csv(self.stg_file_path, "staging")
                trgt_df = self._load_csv(self.trgt_file_path, "target")
            
            # Validate that required dataframes are loaded and not empty
            status = self._validate_data(src_df, stg_df, trgt_df)
            
            return status, src_df, stg_df, trgt_df
        
        except Exception as e:
            print(f"❌ Error loading data for {self.file_name}: {str(e)}")
            return False, None, None, None
    
    def _load_csv(self, file_path, stage_name):
        """
        Load a single CSV file with error handling
        
        Parameters:
        - file_path: Path to CSV file
        - stage_name: Name of the stage (source/staging/target) for logging
        
        Returns:
        - DataFrame: Loaded PySpark DataFrame
        """
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{stage_name.capitalize()} file not found: {file_path}")
        
        print(f"📂 Loading {stage_name} file: {os.path.basename(file_path)}")
        
        # Load CSV with inferSchema
        df = self.ss.read.csv(file_path, inferSchema=True, header=True)
        
        # Quick check if file is empty (more efficient than count())
        if df.rdd.isEmpty():
            raise ValueError(f"{stage_name.capitalize()} file is empty: {file_path}")
        
        # Cache DataFrame for better performance during validations
        df = df.cache()
        
        print(f"✅ {stage_name.capitalize()} loaded successfully")
        return df
    
    def _validate_data(self, src_df, stg_df, trgt_df):
        """
        Validate that required dataframes are loaded based on validation mode
        
        Parameters:
        - src_df: Source DataFrame
        - stg_df: Staging DataFrame
        - trgt_df: Target DataFrame
        
        Returns:
        - bool: True if all required dataframes are loaded, False otherwise
        """
        if self.validation_mode == "source_to_staging":
            return src_df is not None and stg_df is not None
        elif self.validation_mode == "staging_to_target":
            return stg_df is not None and trgt_df is not None
        else:  # both
            return src_df is not None and stg_df is not None and trgt_df is not None