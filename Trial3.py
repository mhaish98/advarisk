import requests
import random
import time
import json
import urllib.parse
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import os
import tkinter as tk
import time
from PIL import Image, ImageTk
import pandas as pd

import os
import openpyxl
from openpyxl import Workbook
import pandas as pd
import sys
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
    {  # 11_1
        "target_scraper": "rj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "29",
            "tehsil_code": "2",
            "village_code": "32880",
            "survey_no": "1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "1"
    },
    {  # 11_2
        "target_scraper": "rj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "13",
            "tehsil_code": "07",
            "village_code": "32234",
            "survey_no": "167/939"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "1"
    },
    {  # 11_3
        "target_scraper": "rj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "01",
            "tehsil_code": "16",
            "village_code": "31469",
            "survey_no": "89/453"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "1"
    },
    {  # 11_4
        "target_scraper": "rj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "20",
            "tehsil_code": "06",
            "village_code": "19979",
            "survey_no": "17/2"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "1"
    },
    {  # 11_5
        "target_scraper": "rj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "24",
            "tehsil_code": "04",
            "village_code": "11244",
            "survey_no": "33/709"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "1"
    }

]

#


# def get_response(callback_url):
#   url = callback_url
#   flag = True
#   keys = []
#   t = 1
#   while t < 6:
#     f = requests.get(url)
#     if f.status_code == 201:
#       data = json.loads(f.text)
#       return data
#
#     elif f.status_code == 404:
#       print(f"Retrying... {t}")
#      # if flag==True:
#       #    flag = False
#       # ws.append([current_date, target_scraper, callback_url, task_id, status_message])
#       t+=1
#       time.sleep(5)


# Create a dictionary to store the callback URL and corresponding target_scraper
callback_url_mapping = []

# Iterate over the payloads
callback_task_mapping = {}

# Create a list to store payloads that need to be retried
retry_payloads = []

# Get the current date in YYYY-MM-DD format
current_date = datetime.now().strftime("%Y%m%d_%H%M%S")

# Create a file name using the current date
response_filename = f"responses_dev_{current_date}.json"
response_filepath = os.path.join("/home/user/Data/Scraping_API/Daily_Sanity/", response_filename)


##############################################################################
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
                # If the value is another dictionary, recursively compare
                matching, unmatched, matched_pairs, unmatched_pairs = compare_json_objects(obj1[key], obj2[key], keys)
                matching_keys += matching
                unmatched_keys += unmatched
                matched_key_value_pairs.update(matched_pairs)
                unmatched_key_value_pairs.update(unmatched_pairs)
            elif isinstance(obj1[key], list) and isinstance(obj2[key], list):
                # If the value is a list, compare list elements
                if len(obj1[key]) == len(obj2[key]):
                    for index, (item1, item2) in enumerate(zip(obj1[key], obj2[key])):
                        if item1 == item2:
                            matching_keys += 1
                            matched_key = '.'.join(keys + [key, str(index)])
                            matched_key_value_pairs[matched_key] = item1
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

#############################################################################

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
            response = requests.post('http://dev-scraper-api.advarisk.com:8090/create_task/', json=payload)
            time.sleep(0.015)
            # import pdb;pdb.set_trace()
            if response.status_code == 200:
                json_response = response.json()
                task_id = json_response.get("task_id")
                if task_id and callback_url:
                    response_data = {
                        "callback_url": callback_url,
                        "task_id": task_id,
                        "target_scraper": payload.get("target_scraper"),
                        "state_code": payload['search_attributes']['state_code'],  # Add this line
                        "district_code": payload['search_attributes']['district_code'],
                        "tehsil_code": payload['search_attributes']['tehsil_code'],
                        #"revenue_circle_code": payload['search_attributes']['search_attributes'],
                        "village_code": payload['search_attributes']['village_code'],
                        #"account_no": payload['search_attributes']['account_no'],
                        "survey_no": payload['search_attributes']['survey_no']
                    }
                    callback_url_mapping.append(response_data)
                    #  callback_task_mapping.append(response_data)
                    json_response.update(response_data)
                    # import pdb; pdb.set_trace()
                with  open(f'/home/user/Data/Scraping_API/Daily_Sanity/{response_filename}', "a") as response_file:

                    json.dump(json_response, response_file)
                    response_file.write(json.dumps(json_response) + "\n")  # Append response to the file
                    response_file.write("\n")  # Add a new line after each payload
                    # callback_task_mapping[callback_url] = task_id  # Map task_id to target_scraper
            else:
                print(f"POST request failed for payload {index}, Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error while sending POST request for payload {index}: {(e)}")

time.sleep(120)

ws.append(["Date", "Target Scraper", "Callback URL", "Task ID", "Status Message", "State Code", "District Code", "Tehsil Code", "Village Code", "Survey No", "Matching Percentage", "Matching Keys", "Unmatched Keys", "Unmatched Data"])


excel_file_path = "/home/user/Data/Scraping_API/Daily_Sanity/Actual_Data/Daily_Sanity_As_is.xlsx"
excel_data = pd.read_excel(excel_file_path)
# Now that all tasks have been created, iterate through the callback URLs and check responses

for i, api_response in enumerate(callback_url_mapping):
    callback_url = urllib.parse.unquote(api_response.get("callback_url"))
    target_scraper = api_response.get("target_scraper")
    task_id = api_response.get("task_id")
    state_code =api_response.get("state_code")  # Extract state_code from the payload
    district_code = api_response.get("district_code")
    tehsil_code = api_response.get("tehsil_code")
    village_code = api_response.get("village_code")
    survey_no = api_response.get("survey_no")
    #revenue_circle_code=api_response.get("revenue_circle_code")
    #account_no=api_response.get("account_no")

    # Send the GET request to the decoded URL
    response = requests.get(callback_url)
    # import pdb;pdb.set_trace()
    # Process the response and display information
    data = json.loads(response.text)
    try:
        excel_file_path = "/home/user/Data/Scraping_API/Daily_Sanity/Actual_Data/Daily_Sanity_As_is.xlsx"
        excel_data = pd.read_excel(excel_file_path)

        response_data = data.get("data", None)
        response_data1 = data

        # if response_data1 is not None:
        # #     Filter out keys from inner_details
        # data_remove =data['data']
        # data_remove.pop('id', None)
        # data_remove.pop('task_id', None)
        # data_remove.pop('order_id', None)
        # data_remove.pop('record_id', None)
        # inner_details = data['data']['scraped_data']['inner_details']
        # inner_details.pop('pdf_link', None)
        # inner_details.pop('html_link', None)
        # scraped_data = data['data']['scraped_data']
        # scraped_data.pop('job_id', None)
        # scraped_data.pop('order_id', None)
        # scraped_data.pop('scraped_time', None)

        webhook_value3 = response_data1
        webhook_value2 = json.dumps(webhook_value3)
        webhook_value1 = webhook_value2.replace(" ", "").replace("\n", "").replace("\t", "")
        webhook_value = json.loads(webhook_value1)
        if response_data:
            status_message = data['data']['attributes']['status_message']
            status = data['data']
            # Find the matching target_scraper in payloads based on callback_url
            matching_payload = next((payload for payload in payloads if payload['callback_url'] == callback_url), None)
            if matching_payload:
                target_scraper = matching_payload.get('target_scraper')

                excel_value3 = excel_data['Response'].values[i]  # Replace 'Value' with your Excel column name
                excel_value2 = excel_value3.replace('\n', '')
                excel_value1 = excel_value2.replace(' ', '')
                excel_value = json.loads(excel_value1)



                # #print(fuzz.token_sort_ratio(excel_value,webhook_value))
                # #print(jsondiff.diff(excel_value,webhook_value))
                # diff = DeepDiff(excel_value, webhook_value, ignore_order=True)
                # # added_items = diff.added()
                # # removed_items = diff.removed()
                # # updated_items = diff.updated()
                # # unchanged_items = diff.unchanged()
                #
                # excel_value_dict = json.loads(excel_value)
                # # webhook_value = json.loads(webhook_value)

                # matched_keys = [key for key in excel_value_dict.keys() if key in webhook_value and excel_value_dict[key] == webhook_value[key]]
                # unmatched_keys = [key for key in excel_value_dict.keys() if key in webhook_value and excel_value_dict[key] != webhook_value[key]] + [key for key in webhook_value.keys() if key not in excel_value_dict]
                #
                # num_matched_keys = len(matched_keys)
                # num_unmatched_keys = len(unmatched_keys)
                excel_key_count = count_keys(excel_value)
                webhook_key_count = count_keys(webhook_value)
                # print("Total key count:", excel_key_count)
                # print("Total key count:", webhook_key_count)

                matching_keys, unmatched_keys, matched_key_value_pairs, unmatched_key_value_pairs = compare_json_objects(webhook_value, excel_value)

                # matching_keys, unmatched_keys = compare_json_objects(excel_value, webhook_value)

                #matching_keys, unmatched_keys, matched_key_value_pairs, unmatched_key_list = compare_json_objects(obj1,obj2)
                matching_percentage = (matching_keys / (matching_keys + unmatched_keys)) * 100

                unmatched_data = "\n".join([f'{key}: {value}' for key, value in unmatched_key_value_pairs.items()])


                print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}, Matching Keys: {matching_keys}, Unmatched Keys: {unmatched_keys}, Percentage of Matching Keys: {matching_percentage:.2f}%")
                # print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message},matching keys:, {matching_keys}, unmatched keys:, {unmatched_keys},Percentage of matching keys:, {{(matching_keys / (matching_keys + unmatched_keys)) * 100:.2f}%}, unmatched keys:, {{(unmatched_keys / (matching_keys + unmatched_keys)) * 100:.2f}%}" )

                ws.append([current_date, target_scraper, callback_url, task_id, status_message,state_code,district_code,tehsil_code,village_code,survey_no ,matching_percentage, matching_keys, unmatched_keys, unmatched_data])

            else:
                print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}")

        else:
            # get_response(callback_url)
            response = requests.get(callback_url)
            print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {eval(response.text)['status']}")
            ws.append([current_date, target_scraper, callback_url, task_id, eval(response.text)["status"]])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        line_number = exc_tb.tb_lineno
        print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Exception: {str(e)}")
        print(f"Exception in {file_name} at line {line_number}: {str(e)}")
        ws.append([current_date, target_scraper, callback_url, task_id, str(e)])

custom_directory = "/home/user/Data/Scraping_API/Daily_Sanity/"
# excel_filename = str(custom_directory)+"responses_prod_"+str(datetime.now().strftime('%Y%m%d_%H%M%S'))+".xlsx"
# str = str(datetime.now().strftime('%Y%m%d_%H%M%S'))
excel_filename = str(custom_directory) + "responses_dev_percent_" + str(datetime.now().strftime('%Y%m%d_%H%M%S')) + ".xlsx"

wb.save(excel_filename)

end_time = time.time()
time_difference = end_time - start_time

hours = int(time_difference // 3600)
minutes = int((time_difference % 3600) // 60)
seconds = int(time_difference % 60)

print(f"Time difference: {hours} hours, {minutes} minutes, {seconds} seconds")

