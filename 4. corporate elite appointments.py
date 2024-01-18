import requests
import pandas as pd
import json
import time
from datetime import datetime
from tqdm import tqdm
from getters import get_corporate_elite
from utils import PROJECT_DIR
from requests.exceptions import ConnectionError

DATA_DIR = f'{PROJECT_DIR}/'

#Upload company data
corporate_elite = get_corporate_elite(DATA_DIR)

#Create list from Series to populate API queries
director_ids = (
    corporate_elite['director_id'].drop_duplicates() 
    .tolist()
)

#Upload API key from main folder
with open(f"{DATA_DIR}/api_key.txt", "r") as file:
    api_key = file.read().strip()

#URL for company officers list
base_url = "https://api.company-information.service.gov.uk/"

dir_list = []
status_codes = {}

request_number = 0
items_per_page = 35  # Set according to the API's maximum allowed value

for director_id in tqdm(director_ids):
    start_index = 0
    total_results = float('inf')

    while start_index < total_results:
        if request_number > 1100:
            print("Sleeping to reset request count")
            time.sleep(300)
            request_number = 0

        search_term = '/officers/' + director_id + '/appointments'

        try:
            response = requests.get(f"{base_url}{search_term}/", auth=(api_key, ''))
            request_number += 1

            status_codes[director_id] = {
                'status_code': response.status_code,
                'timestamp': str(datetime.now()),
                'request_number': request_number
            }

            if response.status_code == 429:
                print("Rate limit reached, sleeping...")
                time.sleep(300)
                request_number = 0
                continue
            elif response.status_code != 200:
                break

            data = json.loads(response.text)
            total_results = data.get('total_results', total_results)  

            dir_info = pd.json_normalize(data['items'])
            dir_info.insert(loc=0, column='director_id', value=director_id)
            dir_list.append(dir_info)

            start_index += items_per_page

        except ConnectionError as e:
            print(f"Connection error occurred: {e}. Retrying...")
            time.sleep(60)  # Wait for a minute before retrying
            continue  # Retry the current loop iteration

status_codes = pd.DataFrame.from_dict(status_codes, orient='index')

corporate_elite_appointments = pd.concat(dir_list, ignore_index=True)

#Export appointments data and search statuses as csvs
corporate_elite_appointments.to_csv(f'{DATA_DIR}/outputs/corporate_elite_appointments.csv', index=False)
status_codes.to_csv(f'{DATA_DIR}/outputs/corporate_elite_appointment_search_statuses.csv', index=True)
