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

# import pdb;pdb.set_trace()
wb = openpyxl.Workbook()
ws = wb.active

start_time = time.time()
timestamp = str(int(time.time()))
current_time = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]

random_number_ORD = str(random.randint(1, 9999)) + str(current_time)

start_time = time.time()

base_callback_url = "https://webhook.advarisk.com/webhook/14/"

callback_url = None


def get_callback_url():
    current_time = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
    callback_url = f"https://webhook.advarisk.com/webhook/14/{current_time}"
    return callback_url

# Define the payloads
payloads = [
{ #1_1
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
        },


  {  # 1_2
        "order_id": "TLN-encumbrance",
        "target_scraper": "tln_encumbrance",
        "geography": "TLN",
        "search_data": {
            "registrar_code": "BHEEMGAL(1803)",  # tehsil_name(tehsil_code)
            "document_number": "1",
            "year": "2023"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
{  # 1_3
        "order_id": "TLN-encumbrance",
        "target_scraper": "tln_encumbrance",
        "geography": "TLN",
        "search_data": {
            "registrar_code": "DOMAKONDA(1806)",  # tehsil_name(tehsil_code)
            "document_number": "1011",
            "year": "2022"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
{  # 1_4
        "order_id": "TLN-encumbrance",
        "target_scraper": "tln_encumbrance",
        "geography": "TLN",
        "search_data": {
            "registrar_code": "CHARMINAR(1608)",  # tehsil_name(tehsil_code)
            "document_number": "101",
            "year": "2023"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  #1_5
        "order_id": "TLN-encumbrance",
        "target_scraper": "tln_encumbrance",
        "geography": "TLN",
        "search_data": {
            "registrar_code": "FAROOQNAGAR(1415)",  # tehsil_name(tehsil_code)
            "document_number": "50",
            "year": "2019"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
{  # 2_1
        "order_id": "tn_encumbrance",
        "target_scraper": "tn_encumbrance",
        "geography": "TN",
        "search_data": {
            "district_name": "Cheyyar|Vellore",
            "district_code": "20048|9",
            "registrar_name": "Vembakkam",
            "registrar_code": "20625:1",
            "village_name": "Venkalathur",
            "village_code": "22940",
            "survey_number": "111/1",
            "encumbrance_start_date": "06-Apr-2018"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 2_2
        "order_id": "tn_encumbrance",
        "target_scraper": "tn_encumbrance",
        "geography": "TN",
        "search_data": {
            "district_name": "Dharmapuri|Salem",
            "district_code": "20026|5",
            "registrar_name": "Dharmapuri Joint I",
            "registrar_code": "20343:1",
            "village_name": "Amanireddihalli",
            "village_code": "14022",
            "survey_number": "48",
            "encumbrance_start_date": "01-Jan-2010"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 2_3
        "order_id": "tn_encumbrance",
        "target_scraper": "tn_encumbrance",
        "geography": "TN",
        "search_data": {
            "district_name": "Chengalpattu| Chennai",
            "district_code": "20004|1",
            "registrar_name": "Acchirapakkam",
            "registrar_code": "20088:1",
            "village_name": "Aathur",
            "village_code": "961",
            "survey_number": "305",
            "encumbrance_start_date": "01-Jan-2010"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 2_4
        "order_id": "tn_encumbrance",
        "target_scraper": "tn_encumbrance",
        "geography": "TN",
        "search_data": {
             "district_name": "Palani|Palani",
            "district_code": "20021|4",
            "registrar_name": "Keeranur",
            "registrar_code": "20276:1",
            "village_name": "Paruthiyur",
            "village_code": "9885",
            "survey_number": "2",
            "encumbrance_start_date": "01-Feb-2010"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 2_5
        "order_id": "tn_encumbrance",
        "target_scraper": "tn_encumbrance",
        "geography": "TN",
        "search_data": {
            "district_name": "Thiruvarur|Tanjore",
            "district_code": "50004|7",
            "registrar_name": "Nannilam",
            "registrar_code": "20458:1",
            "village_name": "Karaiyur",
            "village_code": "18419",
            "survey_number": "2",
            "encumbrance_start_date": "01-Jan-1975"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 3_1
        "order_id": "",
        "target_scraper": "ap_encumbrance",
        "geography": "AP",
        "search_data": {
            "registrar_code": "J.R.GUDEM(504)",
            "document_number": "1",
            "year": "2023"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 3_2
        "order_id": "",
        "target_scraper": "ap_encumbrance",
        "geography": "AP",
        "search_data": {
            "registrar_code": "RAJAM(111)",
            "document_number": "10",
            "year": "2022"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 3_3
        "order_id": "",
        "target_scraper": "ap_encumbrance",
        "geography": "AP",
        "search_data": {
            "registrar_code": "PATHAPATNAM(109)",
            "document_number": "101",
            "year": "2022"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 3_4
        "order_id": "",
        "target_scraper": "ap_encumbrance",
        "geography": "AP",
        "search_data": {
            "registrar_code": "AMADALAVALASA(101)",
            "document_number": "101",
            "year": "2022"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 3_5
        "order_id": "",
        "target_scraper": "ap_encumbrance",
        "geography": "AP",
        "search_data": {
            "registrar_code": "OWK(1314)",
            "document_number": "500",
            "year": "2022"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
        {   #4_1
            "order_id": "",
            "target_scraper": "tln_encumbrance_dharani",
            "geography": "TLN",
            "search_data": {
            "district_code": "13",
            "registrar_code": "264",
            "village_code": "1304017",
            "survey_number": "4/1",
            "document_number": "92"          # khata_number
          },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv"
        },
        {        #4_2
            "order_id": "ORD67476",
            "target_scraper": "tln_encumbrance_dharani",
            "geography": "TLN",
            "search_data": {
            "district_code": "2",
            "registrar_code": "30",
            "village_code": "203017",
            "survey_number": "1/ఖ1",
            "document_number": "273"  # khata_number
          },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv"
        },
        {  # 4_3
            "order_id": "ORD67476",
            "target_scraper": "tln_encumbrance_dharani",
            "geography": "TLN",
            "search_data": {
                "district_code": "7",
                "registrar_code": "639",
                "village_code": "112026",
                "survey_number": "2/అ/2/1/1",
                "document_number": "568"  # khata_number
            },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv"
        },
        {  # 4_4
            "order_id": "ORD67476",
            "target_scraper": "tln_encumbrance_dharani",
            "geography": "TLN",
            "search_data": {
                "district_code": "12",
                "registrar_code": "249",
                "village_code": "1212040",
                "survey_number": "2/అ1",
                "document_number": "101"  # khata_number
            },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv"
        },
        {  # 4_5
            "order_id": "ORD67476",
            "target_scraper": "tln_encumbrance_dharani",
            "geography": "TLN",
            "search_data": {
                "district_code": "10",
                "registrar_code": "194",
                "village_code": "1008007",
                "survey_number": "1D/1",
                "document_number": "60227"  # khata_number
            },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv"
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

# Create a file name using the current date
response_filename = f"responses_stage_enc_{current_date}.json"
response_filepath = os.path.join("/Daily_Sanity/output/", response_filename)


##############################################################################
# def compare_dicts(dict1, dict2, ignore_keys=None):
#     # If ignore_keys is not provided, initialize it as an empty list
#     import pdb; pdb.set_trace()
#     if ignore_keys is None:
#         ignore_keys = []
#     matching_keys = 0
#     # Create copies of the dictionariesith the specified keys removed
#     filtered_dict1 = {key: value for key, value in dict1.items() if key not in ignore_keys}
#     filtered_dict2 = {key: value for key, value in dict2.items() if key not in ignore_keys}
#
#     # Find the keys that are in one dictionary but not in the other
#     unique_keys1 = set(filtered_dict1.keys()) - set(filtered_dict2.keys())
#     unique_keys2 = set(filtered_dict2.keys()) - set(filtered_dict1.keys())
#
#     # Count the matching and unmatched keys
#     matching_keys = len(dict1) - len(unique_keys1)
#     unmatched_keys = len(unique_keys1) + len(unique_keys2)
#
#     # Find and print unmatched key-value pairs
#     unmatched_pairs = {}
#     for key in unique_keys1:
#         unmatched_pairs[key] = filtered_dict1[key]
#     for key in unique_keys2:
#         unmatched_pairs[key] = filtered_dict2[key]
#
#     return matching_keys, unmatched_keys, unmatched_pairs
#
# keys_to_ignore = ["id","order_id","record_id","order_id","pdf_link","scraped_time","task_id"]

#############################################################################
def compare_json_objects(obj1, obj2, keys=None):
    if keys is None:
        keys = []

    matching_keys = 0
    unmatched_keys = 0
    matched_key_value_pairs = {}
    unmatched_key_value_pairs = {}

    # Iterate through keys in the first object
    for key in obj1:
        keys.append(key)

        # Check if the key is present in both objects
        if key in obj2:
            if isinstance(obj1[key], dict) and isinstance(obj2[key], dict):
                # If both values are dictionaries, recursively compare
                matching, unmatched, matched_pairs, unmatched_pairs = compare_json_objects(obj1[key], obj2[key], keys)
                matching_keys += matching
                unmatched_keys += unmatched
                matched_key_value_pairs.update(matched_pairs)
                unmatched_key_value_pairs.update(unmatched_pairs)
            elif isinstance(obj1[key], list) and isinstance(obj2[key], list):
                # If both values are lists, compare list elements
                if len(obj1[key]) == len(obj2[key]):
                    for index, (item1, item2) in enumerate(zip(obj1[key], obj2[key])):
                        if item1 == item2:
                            matching_keys += 1
                            matched_key = '.'.join(keys + [key, str(index)])
                            matched_key_value_pairs[matched_key] = item1
                        elif isinstance(item1, (dict, list)) and isinstance(item2, (dict, list)):
                            # If list elements are dictionaries or lists, recursively compare them
                            matching, unmatched, matched_pairs, unmatched_pairs = compare_json_objects(item1, item2, keys + [key, str(index)])
                            matching_keys += matching
                            unmatched_keys += unmatched
                            matched_key_value_pairs.update(matched_pairs)
                            unmatched_key_value_pairs.update(unmatched_pairs)
                        else:
                            unmatched_keys += 1
                            unmatched_key = '.'.join(keys + [key, str(index)])
                            unmatched_key_value_pairs[unmatched_key] = item1
                else:
                    unmatched_keys += 1
            else:
                # If the values are not dictionaries or lists, compare them
                if obj1[key] == obj2[key]:
                    matching_keys += 1
                    matched_key = '.'.join(keys + [key])
                    matched_key_value_pairs[matched_key] = obj1[key]
                else:
                    unmatched_keys += 1
                    unmatched_key = '.'.join(keys + [key])
                    unmatched_key_value_pairs[unmatched_key] = obj1[key]
        else:
            unmatched_keys += 1
            unmatched_key = '.'.join(keys + [key])
            unmatched_key_value_pairs[unmatched_key] = obj1[key]

        keys.pop()

    return matching_keys, unmatched_keys, matched_key_value_pairs, unmatched_key_value_pairs


######################################################
def count_keys(dictionary):
    count = 0
    if isinstance(dictionary, dict):
        count += len(dictionary)
        for value in dictionary.values():
            count += count_keys(value)
    return count


######################################################################
num_retries = 1

# Iterate over the payloads and send POST requests
# Iterate over the payloads and send POST requests
for retry in range(num_retries):
    for index, payload in enumerate(payloads, start=1):
        try:
            callback_url = get_callback_url()
            payload["callback_url"] = callback_url
            random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
            OrderID = f"ORD{random_number_ORD}"
            payload["order_id"] = OrderID
            response = requests.post('http://stage-scraper-api.advarisk.com:8090/encumbrance-search/', json=payload)
            time.sleep(0.015)
            # import pdb;pdb.set_trace()
            if response.status_code == 200:
                json_response = response.json()
                task_id = json_response.get("task_id")
                if task_id and callback_url:
                    response_data = {
                        "callback_url": callback_url,
                        "task_id": task_id,
                        "geography": payload.get("geography"),
                        "target_scraper": payload.get("target_scraper"),
                        "state_code": payload['search_data'].get('state_code',None),  # Add this line
                        "district_code": payload['search_data'].get('district_code',None),
                        "tehsil_code": payload['search_data'].get('tehsil_code',None),
                        #"revenue_circle_code": payload['search_attributes']['search_attributes'],
                        "village_code": payload['search_data'].get('village_code',None),
                        #"account_no": payload['search_attributes']['account_no'],
                        "survey_no": payload['search_data'].get('survey_number',None),
                        "registrar_code":payload['search_data'].get('registrar_code',None),
                        "document_number":payload['search_data'].get('document_number',None),
                    }
                    callback_url_mapping.append(response_data)
                    #  callback_task_mapping.append(response_data)
                    json_response.update(response_data)
                    # import pdb; pdb.set_trace()
         
            else:
                print(f"POST request failed for payload {index}, Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error while sending POST request for payload {index}: {(e)}")

print(f"Time sleep for 5 minutes started")
time.sleep(300)
print(f"Time sleep for 5 minutes completed")

ws.append(["Date", "Target Scraper", "Callback URL", "Task ID", "Status Message", "Status", "State Code", "District Code", "Tehsil Code", "Village Code", "Survey No", "registrar_code", "document_number", "Matching Percentage", "Matching Keys", "Unmatched Keys", "Unmatched Data", "Exception"])


excel_file_path = "/Daily_Sanity/compare_file/Daily_Sanity_As_is_enc.xlsx"
excel_data = pd.read_excel(excel_file_path)
# Now that all tasks have been created, iterate through the callback URLs and check responses

for i, api_response in enumerate(callback_url_mapping):
    callback_url = urllib.parse.unquote(api_response.get("callback_url"))
    target_scraper = api_response.get("target_scraper")
    task_id = api_response.get("task_id")
    state_code =api_response.get("state_code")  # Extract state_code from the payload
    geography=api_response.get("geography")
    district_code = api_response.get("district_code")
    tehsil_code = api_response.get("tehsil_code")
    village_code = api_response.get("village_code")
    survey_no = api_response.get("survey_no")
    document_number = api_response.get("document_number")
    registrar_code = api_response.get("registrar_code")


    # Send the GET request to the decoded URL
    response = requests.get(callback_url)
    # import pdb;pdb.set_trace()
    # Process the response and display information
    #print(response.text)
    try:
        data = json.loads(response.text)
    except Exception as e:
        ws.append([current_date, target_scraper, callback_url, task_id, "", "Failed",state_code, district_code,tehsil_code, village_code, survey_no,survey_code,"", "", "","", str(e)])
    status_message = None

    try:
        # import pdb; pdb.set_trace()
        excel_file_path = "/Daily_Sanity/compare_file/Daily_Sanity_As_is_enc.xlsx"
        excel_data = pd.read_excel(excel_file_path)

        response_data = data.get("data", None)
        response_data1 = data

        webhook_value3 = response_data1
        webhook_value2 = json.dumps(webhook_value3)
        webhook_value1 = str(webhook_value2).replace(" ", "").replace("\n", "").replace("\t", "")
        webhook_value = json.loads(webhook_value1)
        matching_percentage = 0
        status_message = None
        matching_percentage = None
        matching_keys = 0
        unmatched_keys = 0
        unmatched_data = None

        if response_data:
            status_message = data['data']['attributes']['status_message']
            status_code = data['data']['attributes']['status_code']
            status = data['data']
            # Find the matching target_scraper in payloads based on callback_url
            matching_payload = next((payload for payload in payloads if payload['callback_url'] == callback_url), None)
            if matching_payload:
                target_scraper = matching_payload.get('target_scraper')

                excel_value3 = excel_data['Response'].values[i]  # Replace 'Value' with your Excel column name
                excel_value2 = str(excel_value3).replace('\n', '')
                excel_value1 = str(excel_value2).replace(' ', '')
                excel_value = json.loads(excel_value1)

                excel_key_count = count_keys(excel_value)
                webhook_key_count = count_keys(webhook_value)
                # import pdb; pdb.set_trace()
                matching_keys, unmatched_keys, matched_key_value_pairs, unmatched_key_value_pairs = compare_json_objects(webhook_value, excel_value)
                matching_percentage = 0

                matching_percentage = (matching_keys / (matching_keys + unmatched_keys)) * 100
                unmatched_data = "\n".join([f'{key}: {value}' for key, value in unmatched_key_value_pairs.items()])


                # if unmatched_pairs == {}:
                #     unmatched_pairs = None
                print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}, Matching Keys: {matching_keys}, Unmatched Keys: {unmatched_keys}, Percentage of Matching Keys: {matching_percentage:.2f}%")

                if status_code == "SCR310" or status_code == "SCR314" or status_code == "SCR315" or status_code == "SCR316" or status_code == "SCR320" or status_code == "SCR321" or status_code == "SCR305" or status_code == "SCR326" or status_code == "SCR313" or status_code == "SCR311" or status_code == "SCR305":
                    ws.append([current_date, target_scraper, callback_url, task_id, status_message, "Failed", geography,district_code, tehsil_code, village_code, survey_no, registrar_code, document_number,matching_percentage,matching_keys, unmatched_keys, unmatched_data])
                else:
                    ws.append([current_date, target_scraper, callback_url, task_id, status_message, "Success", geography,district_code, tehsil_code, village_code, survey_no, registrar_code, document_number,matching_percentage,matching_keys, unmatched_keys, unmatched_data])

            else:
                print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}")

        else:
            response = requests.get(callback_url)
            print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {eval(response.text)['status']}")
            ws.append([current_date, target_scraper, callback_url, task_id, status_message, "Failed", geography,district_code, tehsil_code, village_code, survey_no, registrar_code, document_number, matching_percentage,matching_keys, unmatched_keys, unmatched_data,eval(response.text)["status"]])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        line_number = exc_tb.tb_lineno
        print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, {status_message},Exception: {str(e)}")
        print(f"Exception in {file_name} at line {line_number}: {str(e)}")
        ws.append([current_date, target_scraper, callback_url, task_id, status_message, "Failed", geography,district_code, tehsil_code, village_code, survey_no, registrar_code, document_number, matching_percentage,matching_keys, unmatched_keys, unmatched_data,str(e)])

custom_directory = "/Daily_Sanity/output/"
excel_filename = str(custom_directory) + "responses_stage_enc_percent_" + str(datetime.now().strftime('%Y%m%d_%H%M%S')) + ".xlsx"

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
    grouped_data = df.groupby('Target Scraper')['Result'].value_counts().unstack().fillna(0).astype(int)
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
        'Target Scraper': data.get('Target Scraper', "0"),
        'Success': data.get('Success', "0"),
        'Failed': data.get('Failed', "0")
    })
    return table_data


environment = 'STAGE'

data_counts = count_occurrences(excel_file_path_zulip, excel_sheet_name)
table_data = create_table(data_counts)

send_to_zulip(table_data, zulip_stream_name, zulip_topic_name, zulip_email, zulip_api_key, zulip_api_site, environment)
