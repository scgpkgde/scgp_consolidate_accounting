import pandas as pd

def get_sheet_name(file_name): 
    
    excel_file_path = file_name  
    xls = pd.ExcelFile(excel_file_path) 
    lst_sheet = xls.sheet_names
    ans_df = pd.DataFrame(columns=['sheet']) 
    data = {      
             'sheet': lst_sheet
                           
            }
    new_row = pd.DataFrame(data )
    ans_df = pd.concat([ans_df, new_row], ignore_index=True)
    print(ans_df)
    ans_df.to_excel('./list_sheet.xlsx', index = False)
 #---------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    
    #-----------  program statement ------------------------
    # file_name = './file/Program_Statement/1. 01_PS_WY22.xlsb'
    # file_name = './file/Program_Statement/14. C01_PS_PP.xlsx'
    file_name = './file/Program_Statement/15. E01_PS_PPP_Plant.xlsx'
    
    # #-----------  financial ratio --------------------------
    # file_name = './file/Financial_Ratio/8. A09_Financial Ratio_FC.xlsx'
    # file_name = './file/Financial_Ratio/9. C09_Financial Ratio_PP.xlsx'
    # file_name = './file/Financial_Ratio/10. E09_Financial Ratio_PPP.xlsx'
    # file_name = './file/Financial_Ratio/11. D09_Financial Ratio_CIP.xlsx'
    # file_name = './file/Financial_Ratio/12. F09_Financial Ratio_RB.xlsx'
    # file_name = './file/Financial_Ratio/13. Financial Ratio.xlsx'
    
    # #-----------  Exchange rate ----------------------------
    # file_name = './file/Exchange_Rate/18. Exchange rate 2022_send.xls'
    
    # #-----------  cost -------------------------------------
    # file_name = './file/Cost/16. Consol. VC 2022.xlsx'
    # file_name = './file/Cost/17. Database_Cost_E2022.xlsx'
    # file_name = './file/Cost/23. VC 2022_value.xlsx'
    
    get_sheet_name(file_name)