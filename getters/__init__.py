import os
import pandas as pd
  
def get_fame_data(data_dir: str) -> pd.DataFrame:
    """Gets all csv files in the folder and cleans/normalizes columns names
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        List of dataframes
    """
    
    #Empty list to store dataframea
    dataframes = []

    # Loop through all files in the directory
    for file_name in os.listdir(data_dir):
        
        # Construct the full path to the CSV file
        file_path = os.path.join(data_dir, file_name)

        # Read the CSV file into a dataframe
        df = pd.read_csv(file_path)
        
        #Drop the first column of the dataframe
        df = df.iloc[:, 1:]
        
        #Clean/normalize column names
        df.columns = df.columns.str.lower()
        df.columns = [x.replace(" ", "_") for x in df.columns.to_list()]
        df.columns = [x.replace("\n", "") for x in df.columns.to_list()]
        df.columns = [x.replace("operating_revenue_(turnover)th_gbp", "turnover") for x in df.columns]
        df.rename(columns = {'registered_number':'company_no'}, inplace=True)

        # Append the dataframe to the list
        dataframes.append(df)
    
    return dataframes

def get_companies(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of large UK companies compiled from FAME data
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: Company names, numbers etc
    """
    return (pd.read_csv(f"{data_dir}/outputs/companies.csv"))

def get_appointments(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of appointments of largest companies scraped from 
    Companies House
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: Appointments data
    """
    return (pd.read_csv(f"{data_dir}/outputs/dir_info.csv"))

def get_corporate_elite(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of appointments of corporate elite
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: Appointments data
    """
    return (pd.read_csv(f"{data_dir}/outputs/corporate_elite.csv"))

def get_corporate_elite_appointments(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of appointments of corporate elite
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: Appointments data
    """
    return (pd.read_csv(f"{data_dir}/outputs/corporate_elite_appointments.csv"))

def get_charities(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of main charity data
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: main charity data
    """
    return (pd.read_csv(f"{data_dir}/charity_data/publicextract.charity.txt", sep='\t', parse_dates=['date_of_extract', 'date_of_registration',
    'date_of_removal', 'date_cio_dissolution_notice'], on_bad_lines='warn'))

def get_CRCs(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of main charity data
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: main charity data
    """
    return (pd.read_csv(f'{data_dir}/outputs/CRCs.csv', parse_dates=['date_of_registration'], on_bad_lines='warn'))

def get_annual_return_parta(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of charities anuual return parta
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: Charities anuual return parta
    """
    return (pd.read_csv(f"{data_dir}/charity_data/publicextract.charity_annual_return_parta.txt", sep='\t', parse_dates=[0, 3, 4, 6, 7, 8], on_bad_lines='warn'))

def get_royal_patronage(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of charities with royal patronage
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: dataframe of charities with royal patronage
    """
    return (pd.read_csv(f'{data_dir}/royal_patronages.csv', dtype={'registered_charity_number':'int64'}))


def get_area_of_operations_data(data_dir: str) -> pd.DataFrame:
    """
    Gets dataframe of charities area of operations data.
    
    Args:
        data_dir (str): Directory to data.
    
    Returns:
        pd.DataFrame: Charities area of operations data
    """
    file_path = f"{data_dir}/charity_data/publicextract.charity_area_of_operation.txt"
    try:
        return pd.read_csv(file_path, sep='\t', error_bad_lines=False, dtype=str, low_memory=False)
    except Exception as e:
        print(f"Error reading the file: {e}")
        return pd.DataFrame()

def get_authorities_regions_data(data_dir: str) -> pd.DataFrame:
    """
    Gets dataframe of authorities regions data.
    
    Args:
        data_dir (str): Directory to data.
    
    Returns:
        pd.DataFrame: Authorities regions data
    """
    file_path = f"{data_dir}/authorities_regions.csv"
    try:
        # Assuming the file is a CSV with default comma separator.
        # Modify this line if the file format or separator is different.
        return pd.read_csv(file_path, dtype=str, low_memory=False)
    except Exception as e:
        print(f"Error reading the file: {e}")
        return pd.DataFrame()

def get_charity_classification_data(data_dir: str) -> pd.DataFrame:
    """
    Gets dataframe of charity classification data.
    
    Args:
        data_dir (str): Directory to data.
    
    Returns:
        pd.DataFrame: Charity classification data
    """
    file_path = f"{data_dir}/charity_data/publicextract.charity_classification.txt"
    try:
        return pd.read_csv(file_path, sep='\t', error_bad_lines=False, dtype=str, low_memory=False)
    except Exception as e:
        print(f"Error reading the file: {e}")
        return pd.DataFrame()
