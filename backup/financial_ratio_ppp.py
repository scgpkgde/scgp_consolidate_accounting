from initial.financial_ratio.main import ETL
import pandas as pd  
import re 
from datetime import datetime

class financial_ratio(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
#*------------------------------------------------extract data---------------------------------------------------------------------       
    def extract_data(self): 
        
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
                    
                start_col_month +=1 
    
                self.data = ans_df 
 
        except Exception as e:
            print(e) 
#*------------------------------------------------transform data---------------------------------------------------------------------          
    def transform_data(self):
        
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
        self.data = self.data[ (self.data['account'].isin(account))]
        self.data.to_excel(f'./output_data/{self.file_name}', index=False) 
#*------------------------------------------------load data---------------------------------------------------------------------       
    def load_data(self):
        
        print(self.data) 
 
 