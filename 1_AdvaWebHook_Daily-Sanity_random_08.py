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
from difflib import SequenceMatcher
import mysql.connector
import pandas as pd
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
        "target_scraper": "hr_satbara",
        "order_id": "haryana_test-july3",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "01",
            "tehsil_code": "004",
            "village_code": "02812",
            "survey_no": "4//8",
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    }


# Create a dictionary to store the callback URL and corresponding target_scraper
callback_url_mapping = []

# Iterate over the payloads
callback_task_mapping = {}

# Create a list to store payloads that need to be retried
retry_payloads = []

# Get the current date in YYYY-MM-DD format
current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
#############################################################################
num_retries = 1;
state_ids = [{1:[1,10,15,37,954]},{6:[738,743,749,943,745]},{2:[38,43,47,50,64]},{5:[622,627,828,631,617]},{4:[124,131,143,154,156]},{7:[638,649,658,1293,1311]},{3:[89,100,94,117,121]},{3:[93,99,105,111,118]}]  # Replace with your state IDs

state_codes =[{"MH":"mh_satbara"},{"HR":"hr_satbara"},{"MP":"mp_satbara"},{"PB":"pb_satbara"},{"GJ":"gj_satbara"},{"AP":"ap_satbara"},{"RJ":"rj_satbara"},{"RJ":"rj_8a"}]

#district_groups = [district_ids[i:i+5] for i in range(0, len(district_ids), 5)]

############################################################################
# Iterate through the list of payloads
for index, (state_id, state_code) in enumerate(zip(state_ids, state_codes)):
   # print(state_id)
    #print(list(state_id.values())[0])
    # import pdb; pdb.set_trace()
    for i in list(state_id.values())[0]:
        state_idx=list(state_id.keys())[0]
        district_idx= i
        try:
            # payload["district_code"] = district_id
            random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
            OrderID = f"ORD{random_number_ORD}"
            payload["order_id"] = OrderID

            # Add your SQL query logic here
            query = f"""SELECT * FROM survey_meta AS sm LEFT JOIN village_meta AS v ON (v.village_id = sm.village_id) LEFT JOIN taluka_meta AS t ON (t.taluka_id = v.taluka_id) LEFT JOIN district_meta AS d ON (d.district_id = t.district_id) LEFT JOIN state_info AS s ON (s.state_id = d.state_id) WHERE s.state_id = {state_idx} and d.district_id = {district_idx} and sm.is_active = 1 and v.is_active = 1 ORDER BY RAND() LIMIT 1;"""
            cursor.execute(query)
            result = cursor.fetchall()

            if result:
                # Assign the column values to variables
                survey_no = result[0][1]
                survey_code = result[0][3]
                village_meta = result[0][14]
                # village_name = result[0][11]
                tehsil_meta = result[0][27]
                # tehsil_name = result[0][24]
                district_code = result[0][36]
                # district_name = result[0][33]
                # Update payload with the fetched data
                # payload["state_code"] = state_code
                payload["search_attributes"]["district_code"] = district_code
                payload["search_attributes"]["tehsil_code"] = tehsil_meta
                payload["search_attributes"]["village_code"] = village_meta
                payload["search_attributes"]["survey_no"] = survey_no
                payload["search_attributes"]["state_code"] = list(state_code.keys())[0]
                payload["target_scraper"] = list(state_code.values())[0]
                # payload["district_code"] = district_id
                callback_url1 = get_callback_url()
                payload["callback_url"] = callback_url1
                random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
                OrderID = f"ORD{random_number_ORD}"
                payload["order_id"] = OrderID
                # payload["district_name"]= district_name
                # payload["tehsil_name"]= tehsil_name
                # payload["village_name"]= village_name
                # payload["land_owner_name"]= "Amey"
                response_dev = requests.post('http://dev-scraper-api.advarisk.com:8090/create_task/', json=payload)
                time.sleep(0.015)

                callback_url2 = get_callback_url()
                payload["callback_url"] = callback_url2
                random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
                OrderID = f"ORD{random_number_ORD}"
                payload["order_id"] = OrderID

                response_stage = requests.post('http://stage-scraper-api.advarisk.com:8090/create_task/', json=payload)
                time.sleep(0.015)

                callback_url3 = get_callback_url()
                payload["callback_url"] = callback_url3
                random_number_ORD = str(random.randint(1, 9999)) + str(current_time)
                OrderID = f"ORD{random_number_ORD}"
                payload["order_id"] = OrderID
                response_prod = requests.post('http://scraper.advarisk.com:8090/create_task/', json=payload)
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
                        "district_code":district_code,
                        "tehsil_code": tehsil_meta,
                        "village_code": village_meta,
                        "survey_no": survey_no,
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
                        "district_code":district_code,
                        "tehsil_code": tehsil_meta,
                        "village_code": village_meta,
                        "survey_no": survey_no,
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
                        "district_code":district_code,
                        "tehsil_code": tehsil_meta,
                        "village_code": village_meta,
                        "survey_no": survey_no,
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

ws.append(["Date", "Target Scraper", "Callback URL", "Task ID", "ENV","Status Message", "Status","State Code", "District Code","Tehsil Code", "Village Code", "Survey No","Exception"])

# Now that all tasks have been created, iterate through the callback URLs and check responses

for i, api_response in enumerate(callback_url_mapping):
    callback_url = urllib.parse.unquote(api_response.get("callback_url"))
    target_scraper = api_response.get("target_scraper")
    task_id = api_response.get("task_id")
    state_code = api_response.get("state_code")  # Extract state_code from the payload
    district_code = api_response.get("district_code")
    tehsil_code = api_response.get("tehsil_code")
    village_code = api_response.get("village_code")
    survey_no = api_response.get("survey_no")
    ENV = api_response.get("ENV")

    #import pdb; pdb.set_trace()

    # Send the GET request to the decoded URL
    response = requests.get(callback_url)
    try:
        data = json.loads(response.text)
    except Exception as e:
        ws.append([current_date, target_scraper, callback_url, task_id,ENV, "","Failed",state_code, district_code, tehsil_code, village_code, survey_no, str(e)])
    try:
            response_data = data.get("data", None)
            status_message = data['data']['attributes']['status_message']
            status_code = data['data']['attributes']['status_code']
            status = data['data']
            # Find the matching target_scraper in payloads based on callback_url
            print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}")
            if status_code == "SCR310" or status_code == "SCR314" or status_code == "SCR315" or status_code == "SCR316" or status_code == "SCR320" or status_code == "SCR321" or status_code == "SCR305" or status_code == "SCR326" or status_code == "SCR313":
                ws.append([current_date, target_scraper, callback_url, task_id,ENV, status_message, "Failed", state_code, district_code, tehsil_code, village_code, survey_no])
            else:
                ws.append([current_date, target_scraper, callback_url, task_id, ENV, status_message, "Success", state_code, district_code, tehsil_code, village_code, survey_no])

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        line_number = exc_tb.tb_lineno
        print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Exception: {str(e)}")
        print(f"Exception in {file_name} at line {line_number}: {str(e)}")
        ws.append([current_date, target_scraper, callback_url, task_id, ENV,"","Failed",state_code, district_code, tehsil_code, village_code, survey_no, str(e)])

custom_directory = "/Daily_Sanity/output/"

excel_filename = str(custom_directory) + "responses_random_08_" + str(datetime.now().strftime('%Y%m%d_%H%M%S')) + ".xlsx"

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
        'Target Scraper': df.get('Target Scraper', "0"),
        'Environment': df.get("ENV"),
        'Success': df.get('Success', "0"),
        'Failed': df.get('Failed', "0")
    })
    return table_data


data_counts = count_occurrences(excel_file_path_zulip, excel_sheet_name)
table_data = create_table(data_counts)

send_to_zulip(table_data, zulip_stream_name, zulip_topic_name, zulip_email, zulip_api_key, zulip_api_site)
