from initial.program_statement.main import ETL
import pandas as pd
from datetime import datetime

class program_statement(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
        
#*------------------------------------------------extract data---------------------------------------------------------------------
    def extract_data(self): 
        
        try:  
            plant = "PS_PP"
            src = "scgp_cad"
            year = "2022"
            ans_df = pd.DataFrame(columns=self.lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)   
            start_col_month = 6
            month_num = 1
            while True:  
                month = df.iloc[4, start_col_month]  
                start_col_account = 3
                start_row = 5
                
                while True:
                    account = df.iloc[start_row, start_col_account]
                    amount = df.iloc[start_row, start_col_month]
                    
                    data = {      
                            'data_year': year,
                            'data_month':month_num,
                            'account': account,
                            'product': self.sheet,
                            'amount':amount,
                            'update_date': datetime.now(), 
                            'update_by': self.update_by,
                            'is_active':1,
                            'plant': plant,
                            'src': src   
                            }
                    new_row = pd.DataFrame(data, index=[0])
                    ans_df = pd.concat([ans_df, new_row], ignore_index=True)
    
                    if account == 'Total Variable Cost (B/T)': break  
                    
                    start_row += 1
                    
                if month == 'Dec': break 
                
                month_num +=1 
                
                start_col_month +=1 
    
            self.data = ans_df 
 

        except Exception as e:
            print(e) 
            
 #*------------------------------------------------transform data---------------------------------------------------------------------       
    def transform_data(self):
         # Apply the 'transform_account' function to the 'account' column
        self.data['account'] = self.data['account'].apply(self.format_account)
        
#*------------------------------------------------load data----------------------------------------------------------------------
    def load_data(self):
        print(self.data)
        self.data.to_sql(self.table, self.engine, if_exists='append', index=False)  
 