import requests
import random
import time
import json
import urllib.parse
from datetime import datetime

import os
import tkinter as tk
import time
import os
import openpyxl
from openpyxl import Workbook
import pandas as pd
import sys
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from deepdiff import DeepDiff
from difflib import SequenceMatcher
import mysql.connector
from zulip import Client


# import pdb;pdb.set_trace()
wb = openpyxl.Workbook()
ws = wb.active

start_time = time.time()
timestamp = str(int(time.time()))
current_time = datetime.now().strftime("%Y%m%d%H%M%S%f")

config = {
    "user": "prod_property_scraping_app",
    "password": "T9FwFt6QDr5QKPMp",
    "host": "database.advasmart.in",
    "port" : "3307",
    "database": "property_prod",
}

conn = mysql.connector.connect(**config)
cursor = conn.cursor()

callback_url = None

def get_callback_url():
    current_time = datetime.now().strftime("%Y%m%d%H%M%S%f")
    callback_url = f"https://webhook.advarisk.com/webhook/14/{current_time}"
    return callback_url

payload = {
          "order_id": "ORD87476",
          "target_scraper": "tln_encumbrance",
          "geography": "TLN",
          "search_data": {
            "registrar_code" : "ABDULLAPUR(1531)",         #tehsil_name(tehsil_code)
            "document_number": "4",
            "year": "2022"
          },
          "callback_url": callback_url,
          "auth_token": "DS5hp7XnEoKQAkv"
        }
# Create a dictionary to store the callback URL and corresponding target_scraper
callback_url_mapping = []

# Iterate over the payloads
callback_task_mapping = {}

# Create a list to store payloads that need to be retried
retry_payloads = []

# Get the current date in YYYY-MM-DD format
current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
######################################################################

state_ids = [{10:[1,2,3,4,5]}]

state_codes =[{"TLN":"tln_encumbrance"}]

generated_numbers = []


# Iterate through the list of payloads
for index, (state_id, state_code) in enumerate(zip(state_ids, state_codes)):

    #import pdb; pdb.set_trace()
    for i in list(state_id.values())[0]:
        state_idx=list(state_id.keys())[0]
        district_idx= i
        try:
            # payload["district_code"] = district_id
            random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
            OrderID = f"ORD{random_number_ORD}"
            payload["order_id"] = OrderID

            # Add your SQL query logic here
            random_number = random.randint(1, 20000000)
            while random_number in generated_numbers:
                random_number = random.randint(1, 20000000)
            generated_numbers.append(random_number)
            query = f"""SELECT * FROM property_prod.property_telanganatransaction WHERE id ={random_number} ;"""
            cursor.execute(query)
            result = cursor.fetchall()

            if result:
                # Assign the column values to variables
                registrar_code = result[0][1]
                year = result[0][3]
                document_number= result[0][4]
                sro = result[0][2]
                registrar = "{}({})".format(sro, registrar_code)
                # Update payload with the fetched data
                payload["search_data"]["registrar_code"] = registrar
                payload["search_data"]["document_number"] = document_number
                payload["search_data"]["year"] = year

                payload["target_scraper"] = list(state_code.values())[0]

                callback_url1 = get_callback_url()
                payload["callback_url"] = callback_url1
                random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
                OrderID = f"ORD{random_number_ORD}"
                payload["order_id"] = OrderID

                response_dev = requests.post('http://dev-scraper-api.advarisk.com:8090/encumbrance-search/', json=payload)
                time.sleep(0.015)

                callback_url2 = get_callback_url()
                payload["callback_url"] = callback_url2
                random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
                OrderID = f"ORD{random_number_ORD}"
                payload["order_id"] = OrderID

                response_stage = requests.post('http://stage-scraper-api.advarisk.com:8090/encumbrance-search/', json=payload)
                time.sleep(0.015)

                callback_url3 = get_callback_url()
                payload["callback_url"] = callback_url3
                random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
                OrderID = f"ORD{random_number_ORD}"
                payload["order_id"] = OrderID

                response_prod = requests.post('http://scraper.advarisk.com:8090/encumbrance-search/', json=payload)
                time.sleep(0.015)

            # Existing code to process the request response
            if response_dev.status_code == 200:
                json_response1 = response_dev.json()
                task_id1 = json_response1.get("task_id")
                if task_id1 and callback_url1:
                    response_data = {
                        "callback_url": callback_url1,
                        "task_id": task_id1,
                        "target_scraper": list(state_code.values())[0],
                        "state_code": list(state_code.keys())[0],
                        "registrar_code":registrar_code,
                        "year": year,
                        "document_number": document_number,
                        "ENV": "DEV"
                    }
                    callback_url_mapping.append(response_data)
                    json_response1.update(response_data)
            if response_stage.status_code == 200:
                json_response2 = response_stage.json()
                task_id2 = json_response2.get("task_id")
                if task_id2 and callback_url2:
                    response_data = {
                        "callback_url": callback_url2,
                        "task_id": task_id2,
                        "target_scraper": list(state_code.values())[0],
                        "state_code": list(state_code.keys())[0],
                        "registrar_code":registrar_code,
                        "year": year,
                        "document_number": document_number,
                        "ENV": "STAGE"
                    }
                    callback_url_mapping.append(response_data)
                    json_response2.update(response_data)
            if response_prod.status_code == 200:
                json_response3 = response_prod.json()
                task_id3 = json_response3.get("task_id")
                if task_id3 and callback_url3:
                    response_data = {
                        "callback_url": callback_url3,
                        "task_id": task_id3,
                        "target_scraper": list(state_code.values())[0],
                        "state_code": list(state_code.keys())[0],
                        "registrar_code":registrar_code,
                        "year": year,
                        "document_number": document_number,
                        "ENV": "PROD"
                    }
                    callback_url_mapping.append(response_data)
                    json_response3.update(response_data)

            else:
                print(f"POST request failed for payload {state_ids}, district:{state_codes}")
        except Exception as e:
            print(f"Error for payload {index} - State {state_ids}, District {state_codes}: {e}")

# Close the database connection
conn.close()

print(f"Time sleep for 5 minutes started")
time.sleep(300)
print(f"Time sleep for 5 minutes completed")

ws.append(["Date", "Target Scraper", "Callback URL", "Task ID", "ENV","Status Message", "Status","State Code","registrar_code","document_number","year" ,"Exception"])

# Now that all tasks have been created, iterate through the callback URLs and check responses

for i, api_response in enumerate(callback_url_mapping):
    callback_url = urllib.parse.unquote(api_response.get("callback_url"))
    target_scraper = api_response.get("target_scraper")
    task_id = api_response.get("task_id")
    state_code = api_response.get("state_code")  # Extract state_code from the payload
    registrar_code = api_response.get("registrar_code")
    year = api_response.get("year")
    document_number = api_response.get("document_number")
    ENV = api_response.get("ENV")

    # Send the GET request to the decoded URL
    response = requests.get(callback_url)
    try:
        data = json.loads(response.text)
    except Exception as e:
        ws.append([current_date, target_scraper, callback_url, task_id,ENV, "","Failed",state_code,registrar_code,document_number,year , str(e)])
    try:
            response_data = data.get("data", None)
            status_message = data['data']['attributes']['status_message']
            status_code = data['data']['attributes']['status_code']
            status = data['data']
            # Find the matching target_scraper in payloads based on callback_url
            print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}")
            if status_code == "SCR310" or status_code == "SCR314" or status_code == "SCR315" or status_code == "SCR316" or status_code == "SCR320" or status_code == "SCR321" or status_code == "SCR305" or status_code == "SCR326" or status_code == "SCR313":
                ws.append([current_date, target_scraper, callback_url, task_id,ENV, status_message, "Failed", state_code, registrar_code,document_number,year])
            else:
                ws.append([current_date, target_scraper, callback_url, task_id,ENV, status_message, "Success", state_code, registrar_code,document_number,year])

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        line_number = exc_tb.tb_lineno
        print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Exception: {str(e)}",state_code, registrar_code,document_number,year)
        print(f"Exception in {file_name} at line {line_number}: {str(e)}")
        ws.append([current_date, target_scraper, callback_url, task_id,ENV, "","Failed",state_code, registrar_code,document_number,year, str(e)])

custom_directory = "/Daily_Sanity/output/"
#custom_directory = "/home/user/Data/Scraping_API/Daily_Sanity/"


excel_filename = str(custom_directory) + "responses_random_TLN_enc_" + str(datetime.now().strftime('%Y%m%d_%H%M%S')) + ".xlsx"

wb.save(excel_filename)

end_time = time.time()
time_difference = end_time - start_time

hours = int(time_difference // 3600)
minutes = int((time_difference % 3600) // 60)
seconds = int(time_difference % 60)

print(f"Time difference: {hours} hours, {minutes} minutes, {seconds} seconds")

###########################################################################################################################
# Configuration
excel_file_path_zulip = excel_filename
excel_sheet_name = "Sheet"
excel_column_name = "Status"

# Zulip stream information
zulip_stream_name = "scraper-api"
zulip_topic_name = "Daily_Sanity_Updates"
zulip_email = "amey.kulkarni@advarisk.com"
zulip_api_key = "l9gtFX6cXMwH07x1NYWQj0krQz2bCLYe"
zulip_api_site = "https://chat.advarisk.com"


def count_occurrences(file_path, sheet_name):
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Create a new column 'Result' based on 'Status Message'
    df['Result'] = df['Status Message'].apply(lambda x: 'Success' if isinstance(x, str) and x == 'JOB_COMPLETED_SUCCESSFULLY' else 'Failed')

    # Check if 'Success' and 'Failed' are present in the 'Result' column
    # if 'Success' in df['Result'].values and 'Failed' in df['Result'].values:
    grouped_data = df.groupby(['Target Scraper','ENV'])['Result'].value_counts().unstack().fillna(0).astype(int)
    # else:
    # Handle the case where either 'Success' or 'Failed' is missing
    # grouped_data = pd.DataFrame({'Target Scraper': [], 'Success': [], 'Failed': []})
    return grouped_data


def send_to_zulip(data, stream_name, topic_name, zulip_email, zulip_api_key, zulip_api_site):
    client = Client(email=zulip_email, api_key=zulip_api_key, site=zulip_api_site)

    # Format data as a table
    table = data.to_markdown()

    # Send message to Zulip stream with a specific topic
    client.send_message({
        "type": "stream",
        "to": stream_name,
        "subject": topic_name,
        "content": f"```\n{table}\n```"
    })

    #df = pd.read_excel(excel_file_path, sheet_name=excel_sheet_name)

    # data['ENV'] = data['Target Scraper'].apply(lambda x: x.split('_')[-1])

def create_table(data):
    # Reset index to move 'Target Scraper' from index to a regular column
    df = data.reset_index()

    # Create the table DataFrame
    table_data = pd.DataFrame({
        'Target Scraper': df.get('Target Scraper',"0"),
        'Environment':df.get("ENV"),
        'Success': df.get('Success',"0"),
        'Failed': df.get('Failed',"0")
    })
    return table_data


data_counts = count_occurrences(excel_file_path_zulip, excel_sheet_name)
table_data = create_table(data_counts)

send_to_zulip(table_data, zulip_stream_name, zulip_topic_name, zulip_email, zulip_api_key, zulip_api_site)
