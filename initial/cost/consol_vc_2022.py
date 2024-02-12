from initial.cost.main import ETL
import pandas as pd  
import re 
from datetime import datetime

class cost(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
        
#*------------------------------------------------extract data----------------------------------------------------------------------
    def extract_data(self): 
        
       print('consol vc 2022')
       
#*------------------------------------------------transform data----------------------------------------------------------------------  
    def transform_data(self):
        ...
        
#*------------------------------------------------load data----------------------------------------------------------------------
    def load_data(self):
        ...
            