from initial.financial_ratio.main import ETL
import pandas as pd 
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import csv

class financial_ratio(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
    
#*------------------------------------------------extract data---------------------------------------------------------------------      
    def extract_data(self): 
        
        if self.sheet == "BPC BS V":  
            self.extract_sheet_bs()
        
        elif self.sheet == "BPC PL V": 
            self.extract_sheet_pl() 
            
        elif self.sheet == "CIP(ROIC)":  
            self.extract_sheet_roic()
            
#--------------------------------------------------------------    
    def extract_sheet_bs(self):
        
        try:
            lst_expexted_columns = ['gl_account_number', 'gl_account_name',
                                    'gl_month', 'gl_year',
                                    'gl_amount', 'gl_src', 'scd_active',
                                    'scd_start', 'scd_end'
                                    ] 
            
            ans_df = pd.DataFrame(columns=lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)  
            year_month = df.iloc[7, 1] 
            year_month = datetime.strptime(year_month, '%Y %b') 
            self.file_name = 'GL_CIP_'+year_month.strftime('%Y%m')+'.csv'  
            start_row = 4
            
            while True:  
                gl_account_number = df.iloc[start_row, 4] 
                gl_src = df.iloc[2, 145]  
                gl_account_name = df.iloc[start_row, 5]  
                gl_amount = df.iloc[start_row, 145] 
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
                        'scd_active': scd_active, 
                        'scd_start': year_month.strftime('%Y-%m-%d'),
                        'scd_end': scd_end 
                        }
                
                start_row += 1
                
                new_row = pd.DataFrame(data, index=[0])
                ans_df = pd.concat([ans_df, new_row], ignore_index=True)
                
            self.data = ans_df 
            
            self.data.to_csv(f"./output_data/{self.file_name}", index=False)

        except Exception as e:
            print(e) 
        
#--------------------------------------------------------------         
    def extract_sheet_pl(self):
        
        try:
            lst_expexted_columns = ['gl_account_number', 'gl_account_name',
                                    'gl_month', 'gl_year',
                                    'gl_amount', 'gl_src', 'scd_active',
                                    'scd_start', 'scd_end'
                                    ] 
            
            ans_df = pd.DataFrame(columns=lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)  
            year_month = df.iloc[7, 1] 
            year_month = datetime.strptime(year_month, '%Y %b') 
            self.file_name = 'GL_CIP_'+year_month.strftime('%Y%m')+'.csv'  
            start_row = 4
            
            while True:  
                gl_account_number = df.iloc[start_row, 4] 
                gl_account_name = df.iloc[start_row, 5]  
                gl_amount = df.iloc[start_row, 142] 
                gl_src = df.iloc[2, 142]  
                scd_active = 1 
                scd_end = None
                
                if gl_account_number == 'PL': break
                
                data = {      
                        'gl_account_number': gl_account_number,
                        'gl_account_name': gl_account_name,
                        'gl_month': year_month.strftime('%m'),
                        'gl_year': year_month.strftime('%Y'),
                        'gl_amount': gl_amount,
                        'gl_src': gl_src,
                        'scd_active': scd_active, 
                        'scd_start': year_month.strftime('%Y-%m-%d'),
                        'scd_end': scd_end 
                        }
                
                start_row += 1
                
                new_row = pd.DataFrame(data, index=[0])
                ans_df = pd.concat([ans_df, new_row], ignore_index=True)
                
            self.data = ans_df 
            #append csv is exist data
            self.data.to_csv(f"./output_data/{self.file_name}", index=False, mode='a', header=False)
                
        except Exception as e:
            print(e) 
            
#--------------------------------------------------------------         
    def extract_sheet_roic(self): 
        
        try:
            lst_expexted_columns = ['data_year', 'data_month', 'account',
                                     'product', 'amount', 'update_date',
                                     'update_by', 'is_active' 
                                    ]   
            ans_df = pd.DataFrame(columns=lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)    
            product = self.file.split('_')[-1].split('.')[0]  
            self.file_name = f"GL_{product}_ROIC.xlsx"
            start_col_month = 2 
  
            while True:  
                  
                start_col_account = 0
                start_row = 2
                month = df.iloc[1, start_col_month]
                if month == "YTD Jan": break 
                year_month = datetime.strptime(month, "%b'%y") 

                while True:
                    account = df.iloc[start_row, start_col_account]
                    amount = df.iloc[start_row, start_col_month] 
                    data = {      
                            'data_year':year_month.strftime("%Y"),
                            'data_month':year_month.strftime("%m"),
                            'account': account,
                            'product': product,
                            'amount':amount,
                            'update_date': datetime.now(), 
                            'update_by': self.update_by,
                            'is_active':1 
                            }
                    new_row = pd.DataFrame(data, index=[0])
                    ans_df = pd.concat([ans_df, new_row], ignore_index=True)
    
                    if account == 'Finance Cost (exc.Interest Income) : PL05': break  
                    
                    start_row += 1 

                # if month == 'YTD Jan': break 
                
                start_col_month +=1  
                
                self.data = ans_df 
 
        except Exception as e:
            print(e) 
            
#*------------------------------------------------transform data---------------------------------------------------------------------          
    def transform_data(self):
        
        if self.sheet == "CIP(ROIC)": 
            self.data = self.data[pd.notna(self.data['account'])] 
            self.data.fillna(0, inplace=True) 
            account = ['Total Equity',
                    'Cash',
                    'Current Liability Interest Bearing Debt + Non Current Liability',
                    'OPAT',
                    'Non-Recurring Items included in OPAT - Net Tax',
                    'Tax Rate',
                    'Impairment - Net Tax',
                    'Dividend from other company',
                    'NCI - Performance',
                    'NCI - Extra excluded OPAT',
                    'Finance Cost (exc.Interest Income) : PL05' 
                    ]
            self.data = self.data[(self.data['account'].isin(account))]
            self.data.to_excel(f'./output_data/{self.file_name}', index=False) 
        
#*------------------------------------------------load data---------------------------------------------------------------------       
    def load_data(self):
         
        if self.sheet == 'BPC PL V':
            
            try:   
                container_name = 'scgpdldev/EDW_DATA_LANDING/scgp_fi_acct'
                container_client = self.blob_service_client.get_container_client(container= container_name)
                # Upload the file to Azure Blob Storage
                blob_client = container_client.get_blob_client(self.file_name) 
                with open(f'./output_data/{self.file_name}', "rb" ) as data:
                    blob_client.upload_blob(data, overwrite=True)
 
            except Exception as e:
                print(e)
           
            
            
            
    

         