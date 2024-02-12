from abc import ABC, abstractmethod 
import getpass 
from lib import connect_db
from lib import connect_blob
import os
from datetime import datetime
import re
 
class ETL(ABC):
    
    def __init__(self, file, sheet):
        
        self.file = file
        self.sheet = sheet
        self.table = 'scgp_acct_program_statement'
        self.update_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.update_by = str(getpass.getuser())+"@scg.com"
        self.is_active = 1
        self.engine = connect_db.set_connection() 
        self.blob_service_client = connect_blob.set_connection_blob()
        self.scd_active = 1  
        self.scd_end = None  
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

    @staticmethod
    def format_account(account): 
        
        # Replace multiple spaces with a single space, strip leading/trailing spaces, and convert to lowercase
        account = re.sub(r'\s{2,}', ' ', account).strip() 
 
        return account
    
#--------------------------------------------------------------------------------------------------------------------            

class File: 
    @staticmethod
    def check_file(file, sheet):
 
        file_name = os.path.basename(file)
        file_name_without_extension = os.path.splitext(file_name)[0]
        desired_string = file_name_without_extension.split(' ', 1)[1]
 
        if desired_string == "A09_Financial Ratio_FC":
            from initial.financial_ratio.financial_ratio_fc  import financial_ratio 
            return financial_ratio(file, sheet)
        
        elif desired_string == "C09_Financial Ratio_PP":
            from initial.financial_ratio.financial_ratio_pp import financial_ratio 
            return financial_ratio(file, sheet)
        
        elif desired_string == "E09_Financial Ratio_PPP":
            from initial.financial_ratio.financial_ratio_ppp import financial_ratio 
            return financial_ratio(file, sheet)

        elif desired_string == "D09_Financial Ratio_CIP":
            from initial.financial_ratio.financial_ratio_cip import financial_ratio 
            return financial_ratio(file, sheet)
        
        # elif desired_string == "F09_Financial Ratio_RB":
        #     from initial.financial_ratio.financial_ratio_rb import financial_ratio 
        #     return financial_ratio(file, sheet)
        
        elif desired_string == "Financial Ratio":
            from initial.financial_ratio.financial_ratio import financial_ratio 
            return financial_ratio(file, sheet)
        
        
        
        
 
        
      
        
        
        
