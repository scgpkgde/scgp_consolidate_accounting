from initial.financial_ratio.main import ETL
import pandas as pd  
from datetime import datetime

class financial_ratio(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
#*------------------------------------------------extract data---------------------------------------------------------------------       
    def extract_data(self): 
           
        if self.sheet in ['BPC_Dec','BPC_Nov','BPC_Oct','BPC_Sep','BPC_Aug','BPC_Jul','BPC_Jun']:
            self.extract_sheet_bpc()
 
#---------------------------------------------------------- 
    def extract_sheet_bpc(self): 
        
        try:   
            lst_expexted_columns = ['gl_account_number', 'gl_account_name',
                                    'gl_month', 'gl_year',
                                    'gl_amount', 'gl_src'
                                    ] 
            
            ans_df = pd.DataFrame(columns=lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)  
            year_month = '2021'+self.sheet[-3:]
            year_month = datetime.strptime(year_month, '%Y%b') 
            self.file_name = 'GL_PP_'+year_month.strftime('%Y%m')+'.csv'  
            start_column = 6
            
            while True: 
                gl_src = df.iloc[2, start_column]  
                
                if gl_src == 'G330 - Packaging Paper': break
                
                start_row = 4
                
                while True:  
                    gl_account_number = df.iloc[start_row, 4] 
                    gl_account_name = df.iloc[start_row, 5]  
                    gl_amount = df.iloc[start_row, start_column] 
                    scd_active = 1 
                    scd_end = None

                    if gl_account_number == 'BS': break
      
                    data = {      
                            'gl_account_number': gl_account_number,
                            'gl_account_name': gl_account_name,
                            'gl_month': year_month.strftime('%m'),
                            'gl_year': year_month.strftime('%Y'),
                            'gl_amount': gl_amount,
                            'gl_src': gl_src,
                            # 'scd_active': scd_active, 
                            # 'scd_start': year_month.strftime('%Y-%m-%d'),
                            # 'scd_end': scd_end 
                            }
                    
                    new_row = pd.DataFrame(data, index=[0])
                    ans_df = pd.concat([ans_df, new_row], ignore_index=True)
                    
                    start_row += 1
     
                start_column += 1 
 
            self.data = ans_df 

        except Exception as e:
            print(e) 

#*------------------------------------------------transform data---------------------------------------------------------------------          
    def transform_data(self): 
  
        if self.sheet in ['BPC_Dec','BPC_Nov','BPC_Oct','BPC_Sep','BPC_Aug','BPC_Jul','BPC_Jun']:
            self.data['gl_amount'] = self.data['gl_amount'].fillna(0)
            self.data.to_csv(f"./output_data/gl/{self.file_name}", index=False)
            
#*------------------------------------------------load data---------------------------------------------------------------------       
    def load_data(self):
        
        if self.sheet in ['BPC_Dec','BPC_Nov','BPC_Oct','BPC_Sep','BPC_Aug','BPC_Jul','BPC_Jun']:
            try:   
                container_name = 'scgpdldev/EDW_DATA_LANDING/scgp_fi_acct'
                container_client = self.blob_service_client.get_container_client(container= container_name)
                # Upload the file to Azure Blob Storage
                blob_client = container_client.get_blob_client(self.file_name) 
                with open(f'./output_data/gl/{self.file_name}', "rb" ) as data:
                    blob_client.upload_blob(data, overwrite=True)
 
            except Exception as e:
                print(e)
