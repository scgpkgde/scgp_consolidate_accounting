from initial.program_statement.main import ETL
import pandas as pd  
from datetime import datetime
from pyxlsb import open_workbook
class program_statement(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
        
#*------------------------------------------------extract data---------------------------------------------------------------------
    def extract_data(self): 
        
        try:  
            print(f"start sheet: {self.sheet}")
            src = 'SCGP' 
            plant = None
            current_year = datetime.now().year
            first_two_chars = str(current_year)[:2]
            file_name = self.file.split('_')[2].split('.')[0][2:]
            year = first_two_chars+file_name 
            
            ans_df = pd.DataFrame(columns=self.lst_expexted_columns)
            # self.delete_data(year, plant)
            with open_workbook(self.file) as wb:
            # Open the first sheet
                with wb.get_sheet(self.sheet) as sheet:
                
                    for month, column_index in enumerate(range(7, 19), start=1): 
                        target_row_index  = 6
                        
                        for target_row_index in range(6, 52): 
                            target_col_index = column_index  
                            
                            for row_index, row in enumerate(sheet.rows()):
                                    
                                if row_index + 1 == target_row_index:
                                    account = row[4 - 1].v
                                    amount = row[target_col_index - 1].v 
                                    
                                    if  account is None: break 
                                            
                                    data = { 
                                            'data_year':year,
                                            'data_month':month,
                                            'account': account, 
                                            'amount': amount,
                                            'product': self.sheet,  
                                            'update_date':self.update_date,
                                            'update_by':self.update_by,
                                            'is_active':self.is_active,
                                            'plant': plant,
                                            'src':src   
                                            }
                                    
                                    new_row = pd.DataFrame(data, index=[0])
                                    ans_df = pd.concat([ans_df, new_row], ignore_index=True) 
                             
                                    break  # Exit the loop once the target row is found
                                
                        target_row_index = target_row_index+1      
 
            self.data = ans_df     

        except Exception as e:
            print(e)
            
#*------------------------------------------------transform data---------------------------------------------------------------------  
    def transform_data(self):
            
        try:  
            self.data['account'] = self.data['account'].apply(self.format_account)
            self.data['amount'] = self.data['amount'].apply(self.format_amount)    
 
        except Exception as e: 
            print(e)
            
#*------------------------------------------------load data---------------------------------------------------------------------
    def load_data(self):
    
        try:  
            print(self.data) 
            # self.data.to_sql(self.table, self.engine, if_exists='append', index=False) 
            # print("load data complete")
            
        except Exception as e:
            print(e) 
            
            
 
 