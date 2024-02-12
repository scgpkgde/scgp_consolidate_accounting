from initial.financial_ratio.main import ETL
import pandas as pd 
from datetime import datetime

class financial_ratio(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
        
#*------------------------------------------------extract data---------------------------------------------------------------------     

    def extract_data(self): 
        
        if  self.sheet == "FC(ROIC)_FC_Business":  
            self.extract_sheet_roic() 
            
        elif self.sheet == "FC (BS Act)_FC_Business":
            self.extract_sheet_act() 
 
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
                year_month = datetime.strptime(month, "%b'%y")  
                if month == "YTD Jan": break 
  
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
         
                start_col_month += 1 
    
                self.data = ans_df 
 
        except Exception as e:
            print(e) 
            
#-------------------------------------------------------------------------------------   
    def extract_sheet_act(self): 
 
        try:  
            lst_expexted_columns = [
                                    'bs_month', 'bs_year',
                                    'bs_account_name',
                                    'bs_amount', 'bs_src', 'scd_active',
                                    'scd_start', 'scd_end'
                                    ] 
            
            ans_df = pd.DataFrame(columns=lst_expexted_columns) 
            df = pd.read_excel(self.file, sheet_name=self.sheet, header=None)   
            year = df.iloc[3, 2][-4:]
            bs_src = self.file.split('_')[-1].split('.')[0]  
            self.file_name = f"BS_{bs_src}_ACT.xlsx"
            scd_active = 1     
            start_col_month = 2
            month_num = 1
            year_month = year+month_num
            print(year_month)
            while True:  
                month = df.iloc[4, start_col_month] 
                start_col_account =0
                start_row = 6
                
                while True:
                    account_a =  df.iloc[start_row, start_col_account] 
                    account_b =  df.iloc[start_row, start_col_account+1]
                    account_a = "" if pd.isna(account_a) else account_a
                    account_b = "" if pd.isna(account_b) else account_b 
                    bs_account_name =  account_a+account_b 
                    
                    amount = df.iloc[start_row, start_col_month]
                    
                    data = {     
                            'bs_month': month_num,
                            'bs_year': year, 
                            'bs_account_name': bs_account_name,
                            'bs_amount':amount,
                            'bs_src': bs_src,
                            'scd_active': scd_active, 
                            'scd_start': 'year-month',
                            # 'scd_start': year_month.strftime('%Y-%m-%d'),
                            'scd_end': scd_end 
                            }
                    new_row = pd.DataFrame(data, index=[0])
                    ans_df = pd.concat([ans_df, new_row], ignore_index=True)
    
                    if bs_account_name == '   Deferred charges & Others': break  
                    
                    start_row += 1
                    
                if month == 'Dec': break 
                
                month_num +=1 
                
                start_col_month +=1 
    
            self.data = ans_df  

        except Exception as e:
            print(e) 
 
#*------------------------------------------------transform data---------------------------------------------------------------------          
    def transform_data(self):
        
        if  self.sheet == "FC(ROIC)_FC_Business":   
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
            
        elif self.sheet == "FC (BS Act)_FC_Business":
            self.data = self.data[pd.notna(self.data['account'])]
            account = [ 
                    '   Inventories',
                    'Total Current Assets',
                    'Property, Plant And Equipment - At Cost',
                    "Total",
                    'Property, Plant and Equipment - Net',
                    "Other Assets"  
                    ]
            self.data = self.data[~(self.data['account'].isin(account))] 
            self.data.to_excel(f'./output_data/{self.file_name}', index=False)
#*------------------------------------------------load data---------------------------------------------------------------------       
    def load_data(self):
        print(self.data) 
        
 