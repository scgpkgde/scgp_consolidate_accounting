import urllib
from sqlalchemy import create_engine
import configparser
import os 

def set_connection():
        
    try:
        path = '/'.join((os.path.abspath(__file__).replace('\\',
                          '/')).split('/')[:-1])
        config = configparser.ConfigParser()
        config.read(os.path.join(path, 'config.ini'))
        server = config['dest_server']['dest_server']
        database = config['dest_server']['dest_database']
        username = config['dest_server']['dest_username']
        password = config['dest_server']['dest_password']
        password_encoded = urllib.parse.quote_plus(password)
        driver = config['dest_server']['dest_driver']
        conn_str = f'mssql+pyodbc://{username}:{password_encoded}@{server}/{database}?driver={driver}'
        engine = create_engine(conn_str) 
        return engine
    
    except Exception as e:
        print(e)
         