from initial.exchange_rate.main import File as exchange_rate_file
from initial.financial_ratio.main import File as financial_ratio_file
from initial.program_statement.main import File as program_statement_file
from initial.cost.main import File as cost_file
from colorama import Fore, Style

def process_data(data_processor):
    
    try:
        print('start process - extract')
        data_processor.extract_data()
 
        print('start process - transform')
        data_processor.transform_data()
 
        print('start process - load') 
        data_processor.load_data()
        
        print(f"{Style.BRIGHT}{Fore.BLUE}────────────────────────────{Style.RESET_ALL}")

    except Exception as e:
        print(f"{Style.BRIGHT}{Fore.RED} Process Exception: {str(e)} {Style.RESET_ALL}")
 

def main_process(file, sheet, system):
    
    file_mapping = {
        'exchange_rate': exchange_rate_file,
        'financial_ratio': financial_ratio_file,
        'program_statement': program_statement_file,
        'cost': cost_file
    }
    data_processor = file_mapping.get(system, None)
   
    if data_processor:
        data_processor = data_processor.check_file(file, sheet)
        process_data(data_processor)
    else:
        print(f"No such system: {system}")
        
 #*---------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    
    print('============== HELLO START PROGRAM!! =================')
    #*----------------------------------------- program_statement ------------------------------------------------------
    lst_sheet = ['PP']
    file = './file/Program_Statement/1. 01_PS_WY22.xlsb'
    
    # lst_sheet = ['PP_TH',  'SKIC_KF', 'SKIC_PB', 'SKIC_DPP', 'SKIC_PBL',  'Fajar_CB', 'Fajar_Conso', 'Fajar_DPP', 'Fajar_Com', 'DAP_Com']
    # file = './file/Program_Statement/14. C01_PS_PP.xlsx'
    
    # lst_sheet = ['Vexcel']
    # file = './file/Program_Statement/15. E01_PS_PPP_Plant.xlsx' 
    
    system = 'program_statement'   
    
    #*----------------------------------------- Financial_Ratio ------------------------------------------------------
    # lst_sheet = ['FC(ROIC)_FC_Business']
    # lst_sheet = ['FC (BS Act)_FC_Business']
    # file = './file/Financial_Ratio/8. A09_Financial Ratio_FC.xlsx'  
    
    # lst_sheet = ['BPC_Dec', 'BPC_Nov',
    #              'BPC_Oct', 'BPC_Sep', 'BPC_Aug',
    #              'BPC_Jul', 'BPC_Jun']
    # lst_sheet = ['BPC_Dec']
    # lst_sheet = ['PP(ROIC)']
    # file = './file/Financial_Ratio/9. C09_Financial Ratio_PP.xlsx'  
    
    # lst_sheet = ['CPP(ROIC)'] 
    # lst_sheet = ['CPP (BS Act)']
    # file = './file/Financial_Ratio/10. E09_Financial Ratio_PPP.xlsx'  
    
    # lst_sheet = ['CIP(ROIC)']
    # lst_sheet = ['BPC BS V']
    # lst_sheet = ['BPC PL V']
    # lst_sheet = ['BPC BS V', 'BPC PL V']
    # file = './file/Financial_Ratio/11. D09_Financial Ratio_CIP.xlsx' 
    
    
    # lst_sheet = ['RB(ROIC)']
    # file = './file/Financial_Ratio/12. F09_Financial Ratio_RB.xlsx'  
 
    # lst_sheet = ['Conso(ROIC)']
    # file = './file/Financial_Ratio/13. Financial Ratio.xlsx'   
 
    # system = 'financial_ratio'   
    
    #*----------------------------------------- exchange_rate ------------------------------------------------------
    # lst_sheet = [ '2022']
    # file = './file/Exchange_Rate/18. Exchange rate 2022_send.xls'  
    # system = 'exchange_rate'   
    
     #*----------------------------------------- cost ---------------------------------------------------------------
    # lst_sheet = ['TCFP']
    # file = './file/Cost/16. Consol. VC 2022.xlsx'  
    
    # lst_sheet = ['TCFP']
    # file = './file/Cost/17. Database_Cost_E2022.xlsx'  
    
    # lst_sheet = ['TCFP']
    # file = './file/Cost/23. VC 2022_value.xlsx'  
    
    # system = 'cost'
    
    print(f"{Style.BRIGHT}{Fore.YELLOW}process file: {file}{Style.RESET_ALL}")
    
    for sheet in lst_sheet: 
        print(f"{Style.BRIGHT}{Fore.GREEN}process sheet: {sheet}{Style.RESET_ALL}")
        main_process(file, sheet, system)
