from initial.exchange_rate.main import ETL
import pandas as pd  
import re 
from datetime import datetime

class exchange_rate(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
        
#*------------------------------------------------extract data----------------------------------------------------------------------
    def extract_data(self): 
        
        try:
            lst_expexted_columns = ['data_year','month', 'bs_rate',
                                    'pl_rate', 'average_pl', 'country',
                                    'update_date','update_by','is_active']
            ans_df = pd.DataFrame(columns=lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)   
            
            year = df.iloc[1, 0][2:]
            start_col_country = 1
            
            while True:  
                country = df.iloc[1, start_col_country]
                if pd.isna(country): break
                start_row_month = 3
                
                while True: 
                    month = df.iloc[start_row_month, 0]
                    bs_rate = df.iloc[start_row_month, start_col_country]
                    pl_rate = df.iloc[start_row_month, start_col_country+1]
                    average_pl = df.iloc[start_row_month, start_col_country+2]
                    
                    data = {    'data_year': year, 
                                'month': month,
                                'bs_rate':bs_rate,
                                'pl_rate':pl_rate,
                                'average_pl':average_pl,
                                'country': country,
                                'update_date': datetime.now(), 
                                'update_by': self.update_by,
                                'is_active':1 
                            }
                    new_row = pd.DataFrame(data, index=[0])
                    ans_df = pd.concat([ans_df, new_row], ignore_index=True)
                    start_row_month += 1 
                    if month == 'Dec': break 
     
                start_col_country += 3 

            # ans_df.to_excel('./test.xlsx',index = False)
            print(ans_df) 
     
        except Exception as e:
            print(e) 
            
#*------------------------------------------------transform data----------------------------------------------------------------------
    def transform_data(self):
        ...
        
#*------------------------------------------------load data----------------------------------------------------------------------
    def load_data(self):
        ...
            
 
 