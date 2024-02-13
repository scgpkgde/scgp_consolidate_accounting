from initial.financial_ratio.main import ETL
import pandas as pd 
from datetime import datetime

class financial_ratio(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
        
#*------------------------------------------------extract data---------------------------------------------------------------------     

    def extract_data(self):  
            
        if self.sheet == "FC (BS Act)_FC_Business":
            self.extract_act() 
      
#-------------------------------------------------------------------------------------   
    def extract_act(self): 
 
        try:  
            lst_expexted_columns = [
                                    'bs_month', 'bs_year',
                                    'bs_account_name',
                                    'bs_amount', 'bs_src'
                                    ] 
            
            ans_df = pd.DataFrame(columns=lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)   
            year = df.iloc[3, 2][-4:]
            bs_src = self.file.split('_')[-1].split('.')[0]  
            self.file_name = f"BS_{bs_src}_ACT.csv" 
            start_col_month = 2
     
            while True:  
                month = df.iloc[4, start_col_month] 
                start_col_account = 0
                start_row = 6
                
                while True:
                    year_month = year+month
                    year_month = datetime.strptime(year_month, '%Y%b') 
                    account_a =  df.iloc[start_row, start_col_account] 
                    account_b =  df.iloc[start_row, start_col_account+1]
                    account_a = "" if pd.isna(account_a) else account_a
                    account_b = "" if pd.isna(account_b) else account_b 
                    bs_account_name = account_a+account_b 
                    
                    bs_amount = df.iloc[start_row, start_col_month]
                    
                    data = {     
                            'bs_month': year_month.strftime('%m'),
                            'bs_year': year, 
                            'bs_account_name': bs_account_name,
                            'bs_amount': bs_amount,
                            'bs_src': bs_src,
                            # 'scd_active': self.scd_active,  
                            # 'scd_start': year_month.strftime('%Y-%m-%d'),
                            # 'scd_end': self.scd_end 
                            }
                    new_row = pd.DataFrame(data, index=[0])
                    ans_df = pd.concat([ans_df, new_row], ignore_index=True)
    
                    if bs_account_name == '   Deferred charges & Others': break  
                    
                    start_row += 1
                    
                if month == 'Dec': break 
 
                start_col_month +=1 
    
            self.data = ans_df  

        except Exception as e:
            print(e) 
 
#*------------------------------------------------transform data---------------------------------------------------------------------          
    def transform_data(self): 
          
        if self.sheet == "FC (BS Act)_FC_Business":
            self.data = self.data[pd.notna(self.data['bs_account_name'])]
            bs_account_name = [ 
                    '   Inventories',
                    'Total Current Assets',
                    'Property, Plant And Equipment - At Cost',
                    "Total",
                    'Property, Plant and Equipment - Net',
                    "Other Assets"  
                    ]
            self.data = self.data[~(self.data['bs_account_name'].isin(bs_account_name))]
            self.data['bs_amount'] = self.data['bs_amount'].fillna(0) 
            self.data['bs_account_name'] = self.data['bs_account_name'].apply(self.format_account) 
            self.data.to_csv(f'./output_data/bs/{self.file_name}', index=False)
            
#*------------------------------------------------load data---------------------------------------------------------------------       
    def load_data(self):
        try:   
      
            container_name = 'scgpdldev/EDW_DATA_LANDING/scgp_fi_acct'
            container_client = self.blob_service_client.get_container_client(container= container_name)
            # Upload the file to Azure Blob Storage
            blob_client = container_client.get_blob_client(self.file_name) 
            with open(f'./output_data/bs/{self.file_name}', "rb" ) as data:
                blob_client.upload_blob(data, overwrite=True)
 
        except Exception as e:
                print(e)
        
 