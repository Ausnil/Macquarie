import pandas as pd

def validate_excel_file(filepath):
    try:
        xls = pd.ExcelFile(filepath)
        
        # Check required sheets
        required_sheets = {'Customers', 'Transactions', 'Products'}
        if not required_sheets.issubset(set(xls.sheet_names)):
            missing = required_sheets - set(xls.sheet_names)
            return {'valid': False, 'message': f'Missing sheets: {", ".join(missing)}'}
        
        # Check customer format
        customers = pd.read_excel(filepath, sheet_name='Customers')
        if not all(customers.iloc[:, 0].str.startswith('{')) or not all(customers.iloc[:, 0].str.endswith('}')):
            return {'valid': False, 'message': 'Customer data must be in {ID_Name_Email_DOB_Address_Date} format'}
        
        # Check transaction columns
        transactions = pd.read_excel(filepath, sheet_name='Transactions')
        required_trans_cols = {'transaction_id', 'customer_id', 'transaction_date', 'product_code', 'amount'}
        if not required_trans_cols.issubset(set(transactions.columns)):
            return {'valid': False, 'message': 'Transactions sheet missing required columns'}
            
        # Check product columns
        products = pd.read_excel(filepath, sheet_name='Products')
        required_prod_cols = {'product_code', 'product_name', 'category', 'unit_price'}
        if not required_prod_cols.issubset(set(products.columns)):
            return {'valid': False, 'message': 'Products sheet missing required columns'}
        
        return {'valid': True, 'message': 'File is valid'}
    
    except Exception as e:
        return {'valid': False, 'message': f'Validation error: {str(e)}'}
