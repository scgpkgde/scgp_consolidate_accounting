from initial.financial_ratio.main import ETL
import pandas as pd   
from datetime import datetime

class financial_ratio(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
#*------------------------------------------------extract data---------------------------------------------------------------------       
    def extract_data(self):  
            
        if self.sheet == "CPP (BS Act)":
            self.extract_act() 
 #------------------------------------------
    def extract_act(self): 
 
        try:  
            lst_expexted_columns = [
                                    'bs_month', 'bs_year',
                                    'bs_account_name',
                                    'bs_amount', 'bs_src', 'scd_active',
                                    'scd_start', 'scd_end'
                                    ] 
            
            ans_df = pd.DataFrame(columns=lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)   
            
            bs_src = self.file.split('_')[-1].split('.')[0]  
            self.file_name = f"BS_{bs_src}_ACT.xlsx"  
            start_col_month = 4

            while True:  
                month = df.iloc[4, start_col_month]  
                if month == 'Q1': break
                if month == "2021":
                    month = 'Dec'
                    year = '2021'
                else:
                    year = df.iloc[2, 1][-4:]
 
                start_col_account = 1
                start_row = 5
                
                while True:
                    
                    year_month = year+month  
                    year_month = datetime.strptime(year_month,'%Y%b') 
 
                    account_a =  df.iloc[start_row, start_col_account] 
                    account_b =  df.iloc[start_row, start_col_account+1]
                    account_c =  df.iloc[start_row, start_col_account+2]
                    account_a = "" if pd.isna(account_a) else account_a
                    account_b = "" if pd.isna(account_b) else account_b 
                    account_c = "" if pd.isna(account_c) else account_c 
                    bs_account_name =  account_a+account_b+account_c     
                    
                    amount = df.iloc[start_row, start_col_month]
                    
                    data = {     
                            'bs_month': year_month.strftime('%m'),
                            'bs_year': year, 
                            'bs_account_name': bs_account_name,
                            'bs_amount':amount,
                            'bs_src': bs_src,
                            'scd_active': self.scd_active,  
                            'scd_start': year_month.strftime('%Y-%m-%d'),
                            'scd_end': self.scd_end 
                            }
                    new_row = pd.DataFrame(data, index=[0])
                    ans_df = pd.concat([ans_df, new_row], ignore_index=True)
    
                    if bs_account_name == ' Manpower': break  
                    
                    start_row += 1 
 
                start_col_month +=1 
    
            self.data = ans_df  

        except Exception as e:
            print(e) 

#*------------------------------------------------transform data---------------------------------------------------------------------          
    def transform_data(self):
        
        if self.sheet == "CPP (BS Act)":
            
            # Replace empty strings in 'bs_account_name' with 'NA'
            self.data['bs_account_name'].replace('', 'NA', inplace=True)

            # Filter out rows where 'bs_account_name' is 'NA'
            self.data = self.data[self.data['bs_account_name'] != 'NA']
 
            bs_account_name = [ 
                    ' Current Assets',
                    'Total Current Assets', 
                    "Total",
                    'Property, Plant and Equipment - Net',
                    "Total Other Assets" ,
                    'Total Assets'
                    ]
            self.data = self.data[~(self.data['bs_account_name'].isin(bs_account_name))] 
            self.data['bs_amount'] = self.data['bs_amount'].fillna(0)
            self.data['bs_account_name'] = self.data['bs_account_name'].apply(self.format_account) 
            self.data.to_excel(f'./output_data/bs/{self.file_name}', index=False)
              
#*------------------------------------------------load data---------------------------------------------------------------------       
    def load_data(self):
        
        print(self.data) 
 
 