from initial.financial_ratio.main import ETL
import pandas as pd  
import re 
from datetime import datetime

class financial_ratio(ETL):
    def __init__(self, file, sheet):
        super().__init__(file, sheet)
#*------------------------------------------------extract data---------------------------------------------------------------------       
    def extract_data(self): 
       print('ratio')
#*------------------------------------------------transform data---------------------------------------------------------------------          
    def transform_data(self):
        ...
#*------------------------------------------------load data---------------------------------------------------------------------       
    def load_data(self):
        ...
 