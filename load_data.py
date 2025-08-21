class LoadData:
    def __init__(self,ss,file_name,src_file_path,trgt_file_path):
        self.ss=ss
        self.file_name=file_name
        self.src_file_path=src_file_path
        self.trgt_file_path=trgt_file_path
        self.status,self.src_df,self.trgt_df=self.load_data()   
         
    def load_data(self):
        src_df=self.ss.read.csv(self.src_file_path,inferSchema=True,header=True)
        trgt_df=self.ss.read.csv(self.trgt_file_path,inferSchema=True,header=True)
        status= (src_df.count()!=0) and (trgt_df.count()!=0)
        return status,src_df,trgt_df
          
