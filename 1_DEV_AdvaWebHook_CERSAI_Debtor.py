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
import pandas as pd
from zulip import Client
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from deepdiff import DeepDiff
from difflib import SequenceMatcher
from cryptography.fernet import Fernet
from codecs import decode

# import pdb;pdb.set_trace()
wb = openpyxl.Workbook()
ws = wb.active

start_time = time.time()
timestamp = str(int(time.time()))
current_time = datetime.now().strftime("%Y%m%d%H%M%S%f")

random_number_ORD = str(random.randint(1, 9999)) + str(current_time)

start_time = time.time()

###################################################################################
callback_url = None

def get_callback_url():
    current_time = datetime.now().strftime("%Y%m%d%H%M%S%f")
    callback_url = f"https://webhook.advarisk.com/webhook/14/{current_time}"
    return callback_url

#################################################################################
fernet_key = b"XIYVYpjSs3ZY-_m201seinB34SYyYiWtyuk_s4fHm1Q="

def encrypt_string(input_string: str):
    encryptor = Fernet(fernet_key)
    return decode(encryptor.encrypt(input_string.encode("utf-8")))

def decrypt_string(input_string: str):
    decryptor = Fernet(fernet_key)
    return decode(decryptor.decrypt(input_string))

#print(encrypt_string("8421777007paytm"))
upi_id=encrypt_string("8421777007@paytm")

#########################################################################

# Define the payloads
payloads = [
    {#1
        "order_id": "ORD345678",
        "search_data": {
        "debtor_type": "Individual",
        "pan_number": "AGNPR5442G"
    },
        "payment_method": "UPI",
        "user_email_id": "amey.kulkarni@advarisk.com",
        "upi_id":upi_id,
        "callback_url": callback_url,
        "authentication_token": "DS5hp7XnEoKQAkv"
    },
    {#2
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Indian Company",
            "pan_number": "AABCN3493N"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        },
    {#3
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Trust",
            "pan_number": "AACTT0487E"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        },
    {#4
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Partnership Firm",
            "pan_number": "AAAFT1404F"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        },
    {#5
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Indian Company",
            "pan_number": "AAQCS9954C"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        },
    {#6
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Sole Proprietorship",
            "pan_number": "AEOPJ6109B"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        },
    {#7
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Indian Company",
            "pan_number": "AABCG8600Q"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        },
    {#8
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Individual",
            "pan_number": "AAQPP3306G"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        },
    {#9
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Indian Company",
            "pan_number": "AAICS8950L"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        },
    {#10
            "order_id": "ORD345678",
            "search_data": {
            "debtor_type": "Indian Company",
            "pan_number": "AAECG6997M"
        },
            "payment_method": "UPI",
            "user_email_id": "amey.kulkarni@advarisk.com",
            "upi_id":upi_id,
            "callback_url": callback_url,
            "authentication_token": "DS5hp7XnEoKQAkv"
        }
]

# Create a dictionary to store the callback URL and corresponding target_scraper
callback_url_mapping = []

# Iterate over the payloads
callback_task_mapping = {}

# Create a list to store payloads that need to be retried
retry_payloads = []

# Get the current date in YYYY-MM-DD format
current_date = datetime.now().strftime("%Y%m%d_%H%M%S")

######################################################################
num_retries = 1

# Iterate over the payloads and send POST requests
for retry in range(num_retries):
    for index, payload in enumerate(payloads, start=1):
        try:
            callback_url = get_callback_url()
            payload["callback_url"] = callback_url
            random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
            OrderID = f"ORD{random_number_ORD}"
            payload["order_id"] = OrderID
            response = requests.post('http://dev-scraper-api.advarisk.com:8090/cersai/debtor_search/', json=payload)
            time.sleep(0.015)
            # import pdb;pdb.set_trace()
            if response.status_code == 200:
                json_response = response.json()
                task_id = json_response.get("task_id")
                if task_id and callback_url:
                    response_data = {
                        "callback_url": callback_url,
                        "task_id": task_id,
                        "debtor_type": payload['search_data']['debtor_type'],
                        "pan_number": payload['search_data']['pan_number'],
                    }
                    callback_url_mapping.append(response_data)
                    json_response.update(response_data)
            else:
                print(f"POST request failed for payload {index}, Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error while sending POST request for payload {index}: {(e)}")

print(f"Time sleep for 10 minutes started")
time.sleep(600)
print(f"Time sleep for 10 minutes completed")

ws.append(["Date", "Callback_URL", "Task_ID", "Status Message", "Debtor Type","PAN Number","Exception"])

#excel_file_path = "/Daily_Sanity/compare_file/Daily_Sanity_As_is_Stage_prod.xlsx"
#excel_data = pd.read_excel(excel_file_path)
# Now that all tasks have been created, iterate through the callback URLs and check responses

for i, api_response in enumerate(callback_url_mapping):
    callback_url = urllib.parse.unquote(api_response.get("callback_url"))
    task_id = api_response.get("task_id")
    debtor_type = api_response.get("debtor_type")
    pan_number  = api_response.get("pan_number")
    # Send the GET request to the decoded URL
    response = requests.get(callback_url)
    try:
        data = json.loads(response.text)
    except Exception as e:
        ws.append([current_date, callback_url, task_id, "",debtor_type ,pan_number , str(e)])
    try:
        #excel_file_path = "/Daily_Sanity/compare_file/Daily_Sanity_As_is_Stage_prod.xlsx"
        #excel_data = pd.read_excel(excel_file_path)
        #response_data = data.get("data", None)
        #response_data1 = data

        #webhook_value3 = response_data1
        #webhook_value2 = json.dumps(webhook_value3)
        #webhook_value1 = str(webhook_value2).replace(" ", "").replace("\n", "").replace("\t", "")
        #webhook_value = json.loads(webhook_value1)
        status_message = None
        # matching_percentage = None
        # matching_keys = 0
        # unmatched_keys = 0
        # unmatched_data = None
        if response_data:
            status_message = data['data']['attributes']['status_message']
            status_code = data['data']['attributes']['status_code']
            status = data['data']
            # Find the matching target_scraper in payloads based on callback_url
            matching_payload = next((payload for payload in payloads if payload['callback_url'] == callback_url), None)
            if matching_payload:
                #target_scraper = matching_payload.get('target_scraper')

                # excel_value3 = excel_data['Response'].values[i]  # Replace 'Value' with your Excel column name
                # excel_value2 = str(excel_value3).replace('\n', '')
                # excel_value1 = str(excel_value2).replace(' ', '')
                # excel_value = json.loads(excel_value1)

                # excel_key_count = count_keys(excel_value)
                # webhook_key_count = count_keys(webhook_value)
                #
                # matching_keys, unmatched_keys, matched_key_value_pairs, unmatched_key_value_pairs = compare_json_objects(webhook_value, excel_value)
                # matching_percentage = None

                # matching_percentage = (matching_keys / (matching_keys + unmatched_keys)) * 100

                # unmatched_data = "\n".join([f'{key}: {value}' for key, value in unmatched_key_value_pairs.items()])

                #print(f"state: {state}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message})
                ws.append([current_date, callback_url, task_id, status_message,debtor_type,pan_number, str(e)])
            else:
                print(f"Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}")

        else:
            response = requests.get(callback_url)
            #print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, {status_message},Status Message: {eval(response.text)['status']}")
            ws.append([current_date, callback_url, task_id, status_message, debtor_type, pan_number, eval(response.text)["status"]])

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        line_number = exc_tb.tb_lineno
        #print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id},{status_message}, Exception: {str(e)}")
        print(f"Exception in {file_name} at line {line_number}: {str(e)}")
        ws.append([current_date, callback_url, task_id, status_message, debtor_type,pan_number, str(e)])

custom_directory = "/Daily_Sanity/output/"
custom_directory = "/home/user/Data/Scraping_API/Daily_Sanity/"

excel_filename = str(custom_directory) + "responses_dev_cersai_debtor_" + str(datetime.now().strftime('%Y%m%d_%H%M%S')) + ".xlsx"

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
    grouped_data = df.groupby('PAN Number')['Result'].value_counts().unstack().fillna(0).astype(int)
    # else:
    # Handle the case where either 'Success' or 'Failed' is missing
    # grouped_data = pd.DataFrame({'Target Scraper': [], 'Success': [], 'Failed': []})
    return grouped_data


def send_to_zulip(table_data, stream_name, topic_name, zulip_email, zulip_api_key, zulip_api_site, environment):
    client = Client(email=zulip_email, api_key=zulip_api_key, site=zulip_api_site)

    # Format data as a table
    table = table_data.to_markdown()

    # Send message to Zulip stream with a specific topic, including environment information
    message_content = f"Environment: {environment}\n\n```\n{table}\n```"

    client.send_message({
        "type": "stream",
        "to": stream_name,
        "subject": topic_name,
        "content": message_content
    })


def create_table(data):
    # Reset index to move 'Target Scraper' from index to a regular column
    data = data.reset_index()

    # Create the table DataFrame
    table_data = pd.DataFrame({
        'PAN Number': data.get('PAN Number', "0"),
        'Success': data.get('Success', "0"),
        'Failed': data.get('Failed', "0")
    })
    return table_data

environment = 'DEV'

data_counts = count_occurrences(excel_file_path_zulip, excel_sheet_name)
table_data = create_table(data_counts)

send_to_zulip(table_data, zulip_stream_name, zulip_topic_name, zulip_email, zulip_api_key, zulip_api_site, environment)
