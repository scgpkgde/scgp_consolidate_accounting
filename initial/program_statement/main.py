from abc import ABC, abstractmethod
import datetime 
import getpass 
from lib import connect_db
import os
import re
 
class ETL(ABC):
    
    def __init__(self, file, sheet):
         
        self.file = file
        self.sheet = sheet
        self.table = 'scgp_acct_program_statement'
        self.update_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.update_by = str(getpass.getuser())+"@scg.com"
        self.is_active = 1
        self.engine = connect_db.set_connection() 
        self.lst_expexted_columns = ['data_year', 'data_month', 'account',
                                     'product', 'amount', 'update_date',
                                     'update_by', 'is_active', 'plant', 'src'
                                    ]
#--------------------------------------------------------------------------------------------------------------------  
    
    
    
    
    
    @abstractmethod
    def extract_data(self):
        pass
    
    @abstractmethod
    def transform_data(self): 
        pass

    @abstractmethod   
    def load_data(self): 
        pass
    
    
#-----------------------------------------------set format---------------------------------------------------------------------    
    @staticmethod
    def format_account(account):
        # Replace dashes with spaces
        account = account.replace('-', ' ')
        # Replace multiple spaces with a single space, strip leading/trailing spaces, and convert to lowercase
        account = re.sub(r'\s{2,}', ' ', account).strip().lower()
        # Replace spaces with underscores
        account = account.replace(' ', '_')
        
        return account
 
    @staticmethod
    def format_amount(x):
        
        if isinstance(x, (int, float)):
            return "{:20.10f}".format(x * 1000)
        else:
            return x
 
#--------------------------------------------------------------------------------------------------------------------            

class File: 
    @staticmethod
    def check_file(file, sheet):
 
        file_name = os.path.basename(file)
        file_name_without_extension = os.path.splitext(file_name)[0]
        desired_string = file_name_without_extension.split(' ', 1)[1]
        
        if desired_string == "01_PS_WY22":
            from initial.program_statement.ps_wy22 import program_statement 
            return program_statement(file, sheet)
        
        elif desired_string == "C01_PS_PP":
            from initial.program_statement.ps_pp import program_statement 
            return program_statement(file, sheet)
        
        elif desired_string == "E01_PS_PPP_Plant":
            from initial.program_statement.ps_ppp_plant import program_statement 
            return program_statement(file, sheet)
        
        
        
        
 
        
      
        
        
        
