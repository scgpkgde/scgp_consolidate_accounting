from abc import ABC, abstractmethod
import datetime 
import getpass 
from lib import connect_db
import os
 
class ETL(ABC):
    
    def __init__(self, file, sheet):
        
        self.file = file
        self.sheet = sheet
        self.table = 'scgp_acct_program_statement'
        self.update_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.update_by = str(getpass.getuser())+"@scg.com"
        self.is_active = 1
        self.engine = connect_db.set_connection() 
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
 
#--------------------------------------------------------------------------------------------------------------------            

class File: 
    @staticmethod
    def check_file(file, sheet):
 
        file_name = os.path.basename(file)
        file_name_without_extension = os.path.splitext(file_name)[0]
        desired_string = file_name_without_extension.split(' ', 1)[1]
        print(desired_string)
        if desired_string == "Exchange rate 2022_send":
            from initial.exchange_rate.exchange_rate_2022_send  import exchange_rate 
            return exchange_rate(file, sheet)
    
        
        
        
 
        
      
        
        
        
