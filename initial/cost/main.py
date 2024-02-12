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
 
#--------------------------------------------------------------------------------------------------------------------            

class File: 
    @staticmethod
    def check_file(file, sheet):
 
        file_name = os.path.basename(file)
        file_name_without_extension = os.path.splitext(file_name)[0]
        desired_string = file_name_without_extension.split(' ', 1)[1]

        if desired_string == "Consol. VC 2022":
            from initial.cost.consol_vc_2022 import cost 
            return cost(file, sheet)
        
        elif desired_string == "Database_Cost_E2022":
            from initial.cost.database_cost_e2022 import cost 
            return cost(file, sheet)
        
        elif desired_string == "VC 2022_value":
            from initial.cost.vc_2022_value import cost 
            return cost(file, sheet)

 
        
 
        
        
        
 
        
      
        
        
        
