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
    {  # 1_1
        "target_scraper": "hr_satbara",
        "order_id": "haryana_test-july3",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "01",
            "tehsil_code": "004",
            "village_code": "02812",
            "survey_no": "4//8"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 1_2
        "target_scraper": "hr_satbara",
        "order_id": "haryana_test-july3",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "12",
            "tehsil_code": "074",
            "village_code": "03937",
            "survey_no": "2//23/1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 1_3
        "target_scraper": "hr_satbara",
        "order_id": "haryana_test-july3",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "18",
            "tehsil_code": "103",
            "village_code": "03656",
            "survey_no": "8//11/1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 1_4
        "target_scraper": "hr_satbara",
        "order_id": "haryana_test-july3",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "11",
            "tehsil_code": "072",
            "village_code": "00361",
            "survey_no": "1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 1_5
        "target_scraper": "hr_satbara",
        "order_id": "haryana_test-july3",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "06",
            "tehsil_code": "036",
            "village_code": "02320",
            "survey_no": "2//16/1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 2_1
        "target_scraper": "up_satbara",
        "order_id": "test6jan",
        "search_attributes": {
            "state_code": "UP",
            "district_code": "203",
            "tehsil_code": "00913",
            "village_code": "169041",
            "survey_no": "1"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 2_2
        "target_scraper": "up_satbara",
        "order_id": "ORD829348",
        "search_attributes": {
            "state_code": "UP",
            "district_code": "132",
            "tehsil_code": "00704",
            "village_code": "110121",
            "survey_no": "1खम"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 2_3
        "target_scraper": "up_satbara",
        "order_id": "ORD829348",
        "search_attributes": {
            "state_code": "UP",
            "district_code": "145",
            "tehsil_code": "00764",
            "village_code": "124362",
            "survey_no": "1"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 2_4
        "target_scraper": "up_satbara",
        "order_id": "ORD829348",
        "search_attributes": {
            "state_code": "UP",
            "district_code": "170",
            "tehsil_code": "00872",
            "village_code": "155131",
            "survey_no": "37"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 2_5
        "target_scraper": "up_satbara",
        "order_id": "ORD829348",
        "search_attributes": {
            "state_code": "UP",
            "district_code": "197",
            "tehsil_code": "00996",
            "village_code": "219209",
            "survey_no": "68"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 3_1
        "order_id": "ORD435776987",
        "target_scraper": "mp_satbara_v2",
        "search_attributes": {
            "district_code": "23",
            "state_code": "MP",
            "district_name": "इन्दौर|Indore",
            "tehsil_code": "03",
            "tehsil_name": "देपालपुर|Depalpur",
            "village_code": "476019",
            "village_name": "मेठवाड़ा|Methwada",
            "survey_no": "363/2 (S)",
            "survey_code": "123030300065113000675"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 3_2
        "order_id": "ORD435776987",
        "target_scraper": "mp_satbara_v2",
        "search_attributes": {
            "district_code": "44",
            "state_code": "MP",
            "district_name": "आगर मालवा|Agar Malwa",
            "tehsil_code": "01",
            "tehsil_name": "Agar | आगर",
            "village_code": "472515",
            "village_name": "Bet Kheda | बैटखेडा",
            "survey_no": "28 (S)",
            "survey_code": "144010200015027000034"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 3_3
        "order_id": "ORD435776987",
        "target_scraper": "mp_satbara_v2",
        "search_attributes": {
            "district_code": "38",
            "state_code": "MP",
            "district_name": "Balaghat | बालाघाट",
            "tehsil_code": "07",
            "tehsil_name": "Khairlanji | खैरलांजी",
            "village_code": "497468",
            "village_name": "Jhiriya | झिरिया",
            "survey_no": "5 (S)",
            "survey_code": "138070100017207000165"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 3_4
        "order_id": "ORD435776987",
        "target_scraper": "mp_satbara_v2",
        "search_attributes": {
            "district_code": "47",
            "state_code": "MP",
            "district_name": "Umaria | उमरिया",
            "tehsil_code": "06",
            "tehsil_name": "Karkeli | करकेली",
            "village_code": "467659",
            "village_name": "Dhamni | धमनी ",
            "survey_no": "126/2/1/1/3 (S)",
            "survey_code": "1047060300028039000030"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 3_5
        "order_id": "ORD435776987",
        "target_scraper": "mp_satbara_v2",
        "search_attributes": {
            "district_code": "13",
            "state_code": "MP",
            "district_name": "Rewa | रीवा",
            "tehsil_code": "10",
            "tehsil_name": "Jawa | जवा",
            "village_code": "465407",
            "village_name": "Bahera | बहेरा ",
            "survey_no": "4/1 (S)",
            "survey_code": "113100300046378000126"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 4_1
        "target_scraper": "pb_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "PB",
            "district_code": "8",
            "tehsil_code": "48",
            "village_code": "E8D3D096-497D-DC11-A64A-00016C35CBAE",
            "survey_no": "0//93---723"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 4_2
        "target_scraper": "pb_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "PB",
            "district_code": "24",
            "tehsil_code": "166",
            "village_code": "C8CA70B1-1D68-E111-AC3B-001517025079",
            "survey_no": "1//16---4"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 4_3
        "target_scraper": "pb_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "PB",
            "district_code": "15",
            "tehsil_code": "18945",
            "village_code": "1AD0B429-8AF3-DF11-B4C9-001CC0968359",
            "survey_no": "11//14/2---203"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 4_4
        "target_scraper": "pb_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "PB",
            "district_code": "10",
            "tehsil_code": "62",
            "village_code": "5C97E690-A90D-DC11-BF05-000E0CA49FC7",
            "survey_no": "0//112---83"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 4_5
        "target_scraper": "pb_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "PB",
            "district_code": "16",
            "tehsil_code": "111",
            "village_code": "0ECDC59C-CCEB-DF11-85D1-000E0CA4A614",
            "survey_no": "11//25/1/2---525"
        },
        "priority": "0",
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv"
    },
    {  # 5_1
        "target_scraper": "tln_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TLN",
            "district_code": "27",
            "tehsil_code": "502",
            "village_code": "2701001",
            "survey_no": "1/అ",
            "account_no": "48"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 5_2
        "target_scraper": "tln_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TLN",
            "district_code": "34",
            "tehsil_code": "417",
            "village_code": "2206013",
            "survey_no": "1/10",
            "account_no": "9013"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 5_3
        "target_scraper": "tln_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TLN",
            "district_code": "13",
            "tehsil_code": "268",
            "village_code": "1308016",
            "survey_no": "1/అ1",
            "account_no": "210"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 5_4
        "target_scraper": "tln_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TLN",
            "district_code": "24",
            "tehsil_code": "454",
            "village_code": "2413022",
            "survey_no": "3/1",
            "account_no": "515"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 5_5
        "target_scraper": "tln_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TLN",
            "district_code": "6",
            "tehsil_code": "106",
            "village_code": "602002",
            "survey_no": "13/A",
            "account_no": "30000101"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 6_1
        "target_scraper": "tn_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TN",
            "district_code": "02",
            "tehsil_code": "12/N",
            "village_code": "007",
            "survey_no": "1#1A1A3A1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 6_2
        "target_scraper": "tn_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TN",
            "district_code": "21",
            "tehsil_code": "09/Y",
            "village_code": "022",
            "survey_no": "91#2A"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 6_3
        "target_scraper": "tn_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TN",
            "district_code": "07",
            "tehsil_code": "03/N",
            "village_code": "097",
            "survey_no": "44#2A1B"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 6_4
        "target_scraper": "tn_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TN",
            "district_code": "30",
            "tehsil_code": "04/Y",
            "village_code": "009",
            "survey_no": "562#13"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 6_5
        "target_scraper": "tn_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "TN",
            "district_code": "37",
            "tehsil_code": "14/Y",
            "village_code": "013",
            "survey_no": "49#2C"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 7_1
        "target_scraper": "gj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "GJ",
            "district_code": "01",
            "tehsil_code": "01",
            "village_code": "040",
            "survey_no": "1/૧"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 7_2
        "target_scraper": "gj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "GJ",
            "district_code": "12",
            "tehsil_code": "15",
            "village_code": "73",
            "survey_no": "13/p૧/p૨"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 7_3
        "target_scraper": "gj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "GJ",
            "district_code": "21",
            "tehsil_code": "05",
            "village_code": "015",
            "survey_no": "22-બ"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 7_4
        "target_scraper": "gj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "GJ",
            "district_code": "25",
            "tehsil_code": "03",
            "village_code": "026",
            "survey_no": "101"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 7_5
        "target_scraper": "gj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "GJ",
            "district_code": "33",
            "tehsil_code": "09",
            "village_code": "173",
            "survey_no": "66"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 8_1
        "target_scraper": "mh_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "MH",
            "district_code": "7",
            "tehsil_code": "4",
            "village_code": "270700040068810000",
            "survey_no": "6"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 8_2
        "target_scraper": "mh_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "MH",
            "district_code": "31",
            "tehsil_code": "7",
            "village_code": "273100070384450000",
            "survey_no": "1/अ/1अ/प्लॉट नं./15"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 8_3
        "target_scraper": "mh_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "MH",
            "district_code": "19",
            "tehsil_code": "4",
            "village_code": "271900040236220000",
            "survey_no": "54"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 8_4
        "target_scraper": "mh_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "MH",
            "district_code": "29",
            "tehsil_code": "6",
            "village_code": "272900060364390000",
            "survey_no": "29"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 8_5
        "target_scraper": "mh_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "MH",
            "district_code": "36",
            "tehsil_code": "3",
            "village_code": "272100030267080000",
            "survey_no": "43/34"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 9_1
        "target_scraper": "ka_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "KA",
            "district_code": "2",
            "tehsil_code": "4",
            "village_code": "1_*_28",
            "survey_no": "1_*_ಅ"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 9_2
        "target_scraper": "ka_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "KA",
            "district_code": "27",
            "tehsil_code": "1",
            "village_code": "1_*_26",
            "survey_no": "1_*_1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 9_3
        "target_scraper": "ka_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "KA",
            "district_code": "20",
            "tehsil_code": "4",
            "village_code": "1_*_10",
            "survey_no": "1_*_1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 9_4
        "target_scraper": "ka_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "KA",
            "district_code": "23",
            "tehsil_code": "4",
            "village_code": "1_*_26",
            "survey_no": "1_*_*"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 9_5
        "target_scraper": "ka_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "KA",
            "district_code": "12",
            "tehsil_code": "4",
            "village_code": "1_*_7",
            "survey_no": "1_*_A"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 10_1
        "target_scraper": "ap_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "AP",
            "district_code": "1",
            "tehsil_code": "16",
            "village_code": "116040",
            "survey_no": "80-2"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 10_2
        "target_scraper": "ap_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "AP",
            "district_code": "24",
            "tehsil_code": "28",
            "village_code": "2428001",
            "survey_no": "1/1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 10_3
        "target_scraper": "ap_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "AP",
            "district_code": "8",
            "tehsil_code": "43",
            "village_code": "843015",
            "survey_no": "597"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 10_4
        "target_scraper": "ap_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "AP",
            "district_code": "18",
            "tehsil_code": "22",
            "village_code": "1822001",
            "survey_no": "1085"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 10_5
        "target_scraper": "ap_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "AP",
            "district_code": "14",
            "tehsil_code": "10",
            "village_code": "1410005",
            "survey_no": "1592"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
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
        "priority": "0"
    },
    {  # 11_2
        "target_scraper": "rj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "13",
            "tehsil_code": "07",
            "village_code": "37682",
            "survey_no": "585/65"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
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
        "priority": "0"
    },
    {  # 11_4
        "target_scraper": "rj_satbara",
        "order_id": "ALIUP3",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "21",
            "tehsil_code": "12",
            "village_code": "24734",
            "survey_no": "502/1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
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
        "priority": "0"
    },
    {  # 12_1
        "order_id": "ORD7653345",
        "target_scraper": "wb_satbara",
        "search_attributes": {
            "state_code": "WB",
            "district_code": "01",
            "tehsil_code": "01",
            "village_code": "186",
            "survey_no": "33_"  # "_" is delimiter between Survey and Sub-survey
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 12_2
        "order_id": "ORD7653346",
        "target_scraper": "wb_satbara",
        "search_attributes": {
            "state_code": "WB",
            "district_code": "22",
            "tehsil_code": "04",
            "village_code": "174",
            "survey_no": "100_"  # "_" is delimiter between Survey and Sub-survey
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 12_3
        "order_id": "ORD7653347",
        "target_scraper": "wb_satbara",
        "search_attributes": {
            "state_code": "WB",
            "district_code": "04",
            "tehsil_code": "01",
            "village_code": "64",
            "survey_no": "262_"  # "_" is delimiter between Survey and Sub-survey
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 12_4
        "order_id": "ORD7653348",
        "target_scraper": "wb_satbara",
        "search_attributes": {
            "state_code": "WB",
            "district_code": "13",
            "tehsil_code": "17",
            "village_code": "027",
            "survey_no": "270_"  # "_" is delimiter between Survey and Sub-survey
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 12_5
        "order_id": "ORD7653349",
        "target_scraper": "wb_satbara",
        "search_attributes": {
            "state_code": "WB",
            "district_code": "18",
            "tehsil_code": "01",
            "village_code": "079",
            "survey_no": "800_"  # "_" is delimiter between Survey and Sub-survey
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 13_1
        "order_id": "ORD124362198",
        "target_scraper": "jh_satbara",
        "search_attributes": {
            "state_code": "JH",
            "district_code": "04",
            "tehsil_code": "07",
            "revenue_circle_code": "10",
            "village_code": "0257",
            "survey_no": "1214",
            "account_no": "100"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 13_2
        "order_id": "ORD124362198",
        "target_scraper": "jh_satbara",
        "search_attributes": {
            "state_code": "JH",
            "district_code": "21",
            "tehsil_code": "04",
            "revenue_circle_code": "07",
            "village_code": "0112",
            "survey_no": "1000",
            "account_no": "6"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 13_3
        "order_id": "ORD124362198",
        "target_scraper": "jh_satbara",
        "search_attributes": {
            "state_code": "JH",
            "district_code": "13",
            "tehsil_code": "10",
            "revenue_circle_code": "06",
            "village_code": "0093",
            "survey_no": "12/3",
            "account_no": "1"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 13_4
        "order_id": "ORD124362198",
        "target_scraper": "jh_satbara",
        "search_attributes": {
            "state_code": "JH",
            "district_code": "17",
            "tehsil_code": "06",
            "revenue_circle_code": "06",
            "village_code": "08/0030",
            "survey_no": "778",
            "account_no": "11"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 13_5
        "order_id": "ORD124362198",
        "target_scraper": "jh_satbara",
        "search_attributes": {
            "state_code": "JH",
            "district_code": "23",
            "tehsil_code": "07",
            "revenue_circle_code": "04",
            "village_code": "0089",
            "survey_no": "1300",
            "account_no": "17"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 14_1
        "order_id": "ORD09798743",
        "target_scraper": "bh_satbara",
        "search_attributes": {
            "state_code": "BH",
            "district_code": "38",
            "tehsil_code": "2",  # anchal_code
            "revenue_circle_code": "7",  # halka_code
            "village_code": "140",  # mauza_code
            "survey_no": "27/1",  # jamabandi no
            "account_no": "17"  # khata no

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 14_2
        "order_id": "ORD09798743",
        "target_scraper": "bh_satbara",
        "search_attributes": {
            "state_code": "BH",
            "district_code": "7",
            "tehsil_code": "3",  # anchal_code
            "revenue_circle_code": "18",  # halka_code
            "village_code": "145",  # mauza_code
            "survey_no": "1",  # jamabandi no
            "account_no": "1"  # khata no

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 14_3
        "order_id": "ORD09798743",
        "target_scraper": "bh_satbara",
        "search_attributes": {
            "state_code": "BH",
            "district_code": "4",
            "tehsil_code": "11",  # anchal_code
            "revenue_circle_code": "15",  # halka_code
            "village_code": "87",  # mauza_code
            "survey_no": "423",  # jamabandi no
            "account_no": "1"  # khata no

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 14_4
        "order_id": "ORD09798743",
        "target_scraper": "bh_satbara",
        "search_attributes": {
            "state_code": "BH",
            "district_code": "20",
            "tehsil_code": "3",  # anchal_code
            "revenue_circle_code": "5",  # halka_code
            "village_code": "430",  # mauza_code
            "survey_no": "259",  # jamabandi no
            "account_no": "2"  # khata no

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 14_5
        "order_id": "ORD09798743",
        "target_scraper": "bh_satbara",
        "search_attributes": {
            "state_code": "BH",
            "district_code": "23",
            "tehsil_code": "4",  # anchal_code
            "revenue_circle_code": "15",  # halka_code
            "village_code": "471/2",  # mauza_code
            "survey_no": "1",  # jamabandi no
            "account_no": "1"  # khata no

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 15_1
        "order_id": "ORD12345",
        "target_scraper": "ga_satbara",
        "search_attributes": {
            "tehsil_code": "2:1",
            "village_code": "249",
            "survey_no": "100_1",
            "state_code": "GA"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 15_2
        "order_id": "ORD12345",
        "target_scraper": "ga_satbara",
        "search_attributes": {
            "tehsil_code": "1:1",
            "village_code": "16",
            "survey_no": "1_1",
            "state_code": "GA"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 15_3
        "order_id": "ORD12345",
        "target_scraper": "ga_satbara",
        "search_attributes": {
            "tehsil_code": "2:7",
            "village_code": "416",
            "survey_no": "108_15",
            "state_code": "GA"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 15_4
        "order_id": "ORD12345",
        "target_scraper": "ga_satbara",
        "search_attributes": {
            "tehsil_code": "1:6",
            "village_code": "405",
            "survey_no": "19_1",
            "state_code": "GA"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 15_5
        "order_id": "ORD12345",
        "target_scraper": "ga_satbara",
        "search_attributes": {
            "tehsil_code": "2:5",
            "village_code": "313",
            "survey_no": "81_4",
            "state_code": "GA"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 16_1
        "order_id": "ORd1394194",
        "target_scraper": "or_satbara",
        "search_attributes": {
            "district_code": "14",
            "tehsil_code": "1",
            "village_code": "315",
            "revenue_circle_code": "13",
            "survey_no": "1",
            "state_code": "OR"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 16_2
        "order_id": "ORd1394194",
        "target_scraper": "or_satbara",
        "search_attributes": {
            "district_code": "10",
            "tehsil_code": "3",
            "village_code": "355",
            "revenue_circle_code": "11",
            "survey_no": "8",
            "state_code": "OR"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 16_3
        "order_id": "ORd1394194",
        "target_scraper": "or_satbara",
        "search_attributes": {
            "district_code": "11",
            "tehsil_code": "8",
            "village_code": "68",
            "revenue_circle_code": "2",
            "survey_no": "18",
            "state_code": "OR"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 16_4
        "order_id": "ORd1394194",
        "target_scraper": "or_satbara",
        "search_attributes": {
            "district_code": "12",
            "tehsil_code": "1",
            "village_code": "74",
            "revenue_circle_code": "3",
            "survey_no": "80/3",
            "state_code": "OR"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 16_5
        "order_id": "ORd1394194",
        "target_scraper": "or_satbara",
        "search_attributes": {
            "district_code": "13",
            "tehsil_code": "16",
            "village_code": "46",
            "revenue_circle_code": "7",
            "survey_no": "91/1930",
            "state_code": "OR"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 17_1
        "order_id": "testing",
        "target_scraper": "gj_mutation",
        "search_attributes": {"mutation_no": "38", "account_no": "", "state_code": "GJ", "tehsil_code": "02",
                              "village_code": "001", "district_code": "23", "land_owner_name": "Amey"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 17_2
        "order_id": "testing",
        "target_scraper": "gj_mutation",
        "search_attributes": {"mutation_no": "1347", "account_no": "", "state_code": "GJ", "tehsil_code": "08",
                              "village_code": "089", "district_code": "01", "land_owner_name": "Amey"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 17_3
        "order_id": "testing",
        "target_scraper": "gj_mutation",
        "search_attributes": {"mutation_no": "3119", "account_no": "", "state_code": "GJ", "tehsil_code": "06",
                              "village_code": "031", "district_code": "15", "land_owner_name": "Amey"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 17_4
        "order_id": "testing",
        "target_scraper": "gj_mutation",
        "search_attributes": {"mutation_no": "8398", "account_no": "", "state_code": "GJ", "tehsil_code": "02",
                              "village_code": "149", "district_code": "22", "land_owner_name": "Amey"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 17_5
        "order_id": "testing",
        "target_scraper": "gj_mutation",
        "search_attributes": {"mutation_no": "2390", "account_no": "", "state_code": "GJ", "tehsil_code": "09",
                              "village_code": "149", "district_code": "33", "land_owner_name": "Amey"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 18_1
        "order_id": "testing assam",
        "target_scraper": "as_satbara",
        "search_attributes": {
            "district_code": "02",
            "tehsil_code": "02 02",
            "village_code": "02 05 10033",
            "survey_no": "10",
            "state_code": "AS"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 18_2
        "order_id": "testing assam",
        "target_scraper": "as_satbara",
        "search_attributes": {
            "district_code": "33",
            "tehsil_code": "01 04",
            "village_code": "01 09 10002",
            "survey_no": "5",
            "state_code": "AS"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 18_3
        "order_id": "testing assam",
        "target_scraper": "as_satbara",
        "search_attributes": {
            "district_code": "25",
            "tehsil_code": "01 02",
            "village_code": "01 07 10005",
            "survey_no": "10",
            "state_code": "AS"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 18_4
        "order_id": "testing assam",
        "target_scraper": "as_satbara",
        "search_attributes": {
            "district_code": "05",
            "tehsil_code": "01 04",
            "village_code": "03 03 10003",
            "survey_no": "1",
            "state_code": "AS"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 18_5
        "order_id": "testing assam",
        "target_scraper": "as_satbara",
        "search_attributes": {
            "district_code": "17",
            "tehsil_code": "01 03",
            "village_code": "03 03 10034",
            "survey_no": "34",
            "state_code": "AS"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 19_1
        "order_id": "jammu_kashmir_testing",
        "target_scraper": "jk_satbara",
        "search_attributes": {"state_code": "JK",
                              "district_code": "14",
                              "district_name": "Anantnag",
                              "tehsil_code": "181",
                              "tehsil_name": "Anantnag",
                              "village_code": "3858",
                              "village_name": "Anzoowallah",
                              "year": "2018-19",
                              "survey_no": "1"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 19_2
        "order_id": "jammu_kashmir_testing",
        "target_scraper": "jk_satbara",
        "search_attributes": {"state_code": "JK",
                              "district_code": "1",
                              "district_name": "Kupwara",
                              "tehsil_code": "70",
                              "tehsil_name": "Machil",
                              "village_code": "370",
                              "village_name": "Pushwari",
                              "year": "2015-16",
                              "survey_no": "936/587"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 19_3
        "order_id": "jammu_kashmir_testing",
        "target_scraper": "jk_satbara",
        "search_attributes": {"state_code": "JK",
                              "district_code": "21",
                              "district_name": "Jammu",
                              "tehsil_code": "254",
                              "tehsil_name": "Dansal",
                              "village_code": "6090",
                              "village_name": "Dhani",
                              "year": "2015-16",
                              "survey_no": "275"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 19_4
        "order_id": "jammu_kashmir_testing",
        "target_scraper": "jk_satbara",
        "search_attributes": {"state_code": "JK",
                              "district_code": "8",
                              "district_name": "Baramulla",
                              "tehsil_code": "135",
                              "tehsil_name": "Khoie",
                              "village_code": "2526",
                              "village_name": "Panzipora",
                              "year": "2016-17",
                              "survey_no": "531"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 19_5
        "order_id": "jammu_kashmir_testing",
        "target_scraper": "jk_satbara",
        "search_attributes": {"state_code": "JK",
                              "district_code": "19",
                              "district_name": "Udhampur",
                              "tehsil_code": "233",
                              "tehsil_name": "Latti",
                              "village_code": "5261",
                              "village_name": "Padder",
                              "year": "2016-17",
                              "survey_no": "981/509/435/183"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 20_1
        "order_id": "rj_bhunaksha",
        "target_scraper": "rj_bhunaksha",
        "search_attributes": {"district_code": "31", "tehsil_code": "02", "village_code": "15899",
                              "survey_no": "209/342#5_25", "state_code": "RJ"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 20_2
        "order_id": "rj_bhunaksha",
        "target_scraper": "rj_bhunaksha",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "13",
            "tehsil_code": "07",
            "village_code": "32279",
            "survey_no": "601/2273"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 20_3
        "order_id": "rj_bhunaksha",
        "target_scraper": "rj_bhunaksha",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "01",
            "tehsil_code": "16",
            "village_code": "31469",
            "survey_no": "89/453"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 20_4
        "order_id": "rj_bhunaksha",
        "target_scraper": "rj_bhunaksha",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "20",
            "tehsil_code": "06",
            "village_code": "19979",
            "survey_no": "17/2"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 20_5
        "order_id": "rj_bhunaksha",
        "target_scraper": "rj_bhunaksha",
        "search_attributes": {
            "state_code": "RJ",
            "district_code": "24",
            "tehsil_code": "04",
            "village_code": "11244",
            "survey_no": "33/709"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {   #21_1
        "order_id": "ka_service53",
        "target_scraper": "ka_service53",
        "search_attributes": {
            "state_code":"KA",
            "district_name":"Banglore",
            "district_code":"2",
            "tehsil_code":"1",
            "village_code":"1_*_3",
            "survey_no":"1_*_13/2ಬ"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {   #21_2
        "order_id": "ka_service53",
        "target_scraper": "ka_service53",
        "search_attributes": {
            "state_code":"KA",
            "district_name":"CHIKKABALLAPUR",
            "district_code":"28",
            "tehsil_code":"2",
            "village_code":"1_*_84",
            "survey_no":"1_*_1"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {   #21_3
        "order_id": "ka_service53",
        "target_scraper": "ka_service53",
        "search_attributes": {
            "state_code":"KA",
            "district_name":"CHIKMAGALUR",
            "district_code":"17",
            "tehsil_code":"1",
            "village_code":"2_*_23",
            "survey_no":"1_*_10"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {   #21_4
        "order_id": "ka_service53",
        "target_scraper": "ka_service53",
        "search_attributes": {
            "state_code":"KA",
            "district_name":"VIJAYANAGARA",
            "district_code":"31",
            "tehsil_code":"2",
            "village_code":"3_*_23",
            "survey_no":"2_*_*"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {   #21_5
        "order_id": "ka_service53",
        "target_scraper": "ka_service53",
        "search_attributes": {
            "state_code":"KA",
            "district_name":"KOLAR",
            "district_code":"19",
            "tehsil_code":"12",
            "village_code":"3_*_8",
            "survey_no":"3_*_*"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 22_1
        "order_id": "uksatbara",
        "target_scraper": "uk_satbara",
        "search_attributes": {"district_code": "056", "district_name": "उत्तरकाशी", "tehsil_name": "पुरोला_*_पुरोला",
                              "tehsil_code": "00278_*_56785", "village_code": "040101", "village_name": "वेस्टी वल्ली",
                              "survey_no": "12", "state_code": "UK"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 22_2
        "order_id": "uksatbara",
        "target_scraper": "uk_satbara",
        "search_attributes": {"district_code": "064", "district_name": "अल्‍मोडा", "tehsil_name": "मछोड़ा_*_सल्ट",
                              "tehsil_code": "06339_*_68588", "village_code": "052258", "village_name": "टोटाम",
                              "survey_no": "2", "state_code": "UK"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 22_3
        "order_id": "uksatbara",
        "target_scraper": "uk_satbara",
        "search_attributes": {"district_code": "065", "district_name": "चम्पावत",
                              "tehsil_name": "पूर्णागिरी_*_काली कुमाऊँ", "tehsil_code": "00337_*_65374",
                              "village_code": "054453", "village_name": "चन्दनी", "survey_no": "26व", "state_code": "UK"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 22_4
        "order_id": "uksatbara",
        "target_scraper": "uk_satbara",
        "search_attributes": {"district_code": "060", "district_name": "देहरादून", "tehsil_name": "डोईवाला_*_परवादून",
                              "tehsil_code": "00356_*_60063", "village_code": "045303", "village_name": "कुढाल",
                              "survey_no": "60क", "state_code": "UK"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 22_5
        "order_id": "uksatbara",
        "target_scraper": "uk_satbara",
        "search_attributes": {"district_code": "068", "district_name": "हरिद्वार", "tehsil_name": "भगवानपुर_*_पुरोला",
                              "tehsil_code": "00357_*_68563", "village_code": "056336", "village_name": "हसनावाला",
                              "survey_no": "78", "state_code": "UK"
                              },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 23_1
        "order_id": "hp_stringtesting",
        "target_scraper": "hp_satbara",
        "search_attributes": {"district_code": "04", "tehsil_code": "08-10.126.110.16", "village_code": "007201",
                              "survey_no": "7", "state_code": "HP"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 23_2
        "order_id": "hp_stringtesting",
        "target_scraper": "hp_satbara",
        "search_attributes": {"district_code": "12", "tehsil_code": "05-10.126.110.16", "village_code": "017201",
                              "survey_no": "295/1", "state_code": "HP"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 23_3
        "order_id": "hp_stringtesting",
        "target_scraper": "hp_satbara",
        "search_attributes": {"district_code": "03", "tehsil_code": "10-10.126.110.16", "village_code": "017001",
                              "survey_no": "2404/2400/867", "state_code": "HP"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 23_4
        "order_id": "hp_stringtesting",
        "target_scraper": "hp_satbara",
        "search_attributes": {"district_code": "06", "tehsil_code": "29-10.126.110.16", "village_code": "020301",
                              "survey_no": "487", "state_code": "HP"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 23_5
        "order_id": "hp_stringtesting",
        "target_scraper": "hp_satbara",
        "search_attributes": {"district_code": "11", "tehsil_code": "09-10.126.110.16", "village_code": "005001",
                              "survey_no": "17", "state_code": "HP"},
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 24_1
        "order_id": "ORD98134798324",
        "target_scraper": "cg_satbara",
        "search_attributes": {
            "district_code": "21",
            "tehsil_code": "228",
            "village_code": "5702257",
            "survey_no": "101",
            "state_code": "CG"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 24_2
        "order_id": "ORD98134798324",
        "target_scraper": "cg_satbara",
        "search_attributes": {
            "district_code": "8",
            "tehsil_code": "40",
            "village_code": "5502108",
            "survey_no": "10/2",
            "state_code": "CG"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 24_3
        "order_id": "ORD98134798324",
        "target_scraper": "cg_satbara",
        "search_attributes": {
            "district_code": "25",
            "tehsil_code": "228",
            "village_code": "5702246",
            "survey_no": "4/4",
            "state_code": "CG"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 24_4
        "order_id": "ORD98134798324",
        "target_scraper": "cg_satbara",
        "search_attributes": {
            "district_code": "21",
            "tehsil_code": "193",
            "village_code": "5202118",
            "survey_no": "196/2",
            "state_code": "CG"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 24_5
        "order_id": "ORD98134798324",
        "target_scraper": "cg_satbara",
        "search_attributes": {
            "district_code": "31",
            "tehsil_code": "229",
            "village_code": "5408088",
            "survey_no": "12/1",
            "state_code": "CG"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 25_1
        "order_id": "ORD86872434",
        "target_scraper": "hr_satbara_ownerwise",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "19",
            "tehsil_code": "134",
            "village_code": "00265",
            "land_owner_name": "हरियाणा सरकार",
            "account_no": "278"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 25_2
        "order_id": "ORD86872434",
        "target_scraper": "hr_satbara_ownerwise",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "15",
            "tehsil_code": "092",
            "village_code": "01582",
            "land_owner_name": "आबादी देह",
            "account_no": "726"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 25_3
        "order_id": "ORD86872434",
        "target_scraper": "hr_satbara_ownerwise",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "05",
            "tehsil_code": "137",
            "village_code": "04086",
            "land_owner_name": "आबादी देह",
            "account_no": "643"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 25_4
        "order_id": "ORD86872434",
        "target_scraper": "hr_satbara_ownerwise",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "21",
            "tehsil_code": "126",
            "village_code": "01499",
            "land_owner_name": "अंकित चंदिला",
            "account_no": "720"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 25_5
        "order_id": "ORD86872434",
        "target_scraper": "hr_satbara_ownerwise",
        "search_attributes": {
            "state_code": "HR",
            "district_code": "10",
            "tehsil_code": "064",
            "village_code": "06191",
            "land_owner_name": "अंगुरी देवी",
            "account_no": "18"

        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"

    },
    {  # 27_1
        "order_id": "ORD9378647",
        "target_scraper": "ap_webland",
        "search_attributes": {
            "survey_code": "11/14ఎ",
            "state_code": "AP",
            "tehsil_code": "50",
            "village_code": "1050014",
            "district_code": "10"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {#27_2
        "order_id": "ORD9378647",
        "target_scraper": "ap_webland",
        "search_attributes": {
            "survey_code": "2",
            "state_code": "AP",
            "tehsil_code": "65",
            "village_code": "1065013",
            "district_code": "10"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {#27_3
        "order_id": "ORD9378647",
        "target_scraper": "ap_webland",
        "search_attributes": {
            "survey_code": "3/2C2",
            "state_code": "AP",
            "tehsil_code": "8",
            "village_code": "2008003",
            "district_code": "20"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {#27_4
        "order_id": "ORD9378647",
        "target_scraper": "ap_webland",
        "search_attributes": {
            "survey_code": "7-2",
            "state_code": "AP",
            "tehsil_code": "11",
            "village_code": "2611019",
            "district_code": "26"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {#27_5
        "order_id": "ORD9378647",
        "target_scraper": "ap_webland",
        "search_attributes": {
            "survey_code": "17-1-B-1-A",
            "state_code": "AP",
            "tehsil_code": "1",
            "village_code": "1601002",
            "district_code": "16"
        },
        "callback_url": callback_url,
        "auth_token": "DS5hp7XnEoKQAkv",
        "priority": "0"
    },
    {  # 28_1
            "target_scraper": "rj_8a",
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
            "priority": "0"
        },
        {  # 28_2
            "target_scraper": "rj_8a",
            "order_id": "ALIUP3",
            "search_attributes": {
                "state_code": "RJ",
                "district_code": "13",
                "tehsil_code": "07",
                "village_code": "37682",
                "survey_no": "1"
            },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
        },
        {  # 28_3
            "target_scraper": "rj_8a",
            "order_id": "ALIUP3",
            "search_attributes": {
                "state_code": "RJ",
                "district_code": "01",
                "tehsil_code": "16",
                "village_code": "31469",
                "survey_no": "1"
            },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
        },
        {  # 28_4
            "target_scraper": "rj_8a",
            "order_id": "ALIUP3",
            "search_attributes": {
                "state_code": "RJ",
                "district_code": "16",
                "tehsil_code": "09",
                "village_code": "59462",
                "survey_no": "1"
            },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
        },
        {  # 28_5
            "target_scraper": "rj_8a",
            "order_id": "ALIUP3",
            "search_attributes": {
                "state_code": "RJ",
                "district_code": "24",
                "tehsil_code": "04",
                "village_code": "11244",
                "survey_no": "1"
            },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
        },
    {
      "target_scraper": "gj_upin",
      "order_id": "ORD4091843143148353",
      "search_attributes": {
        "state_code": "GJ",
        "upin": "13001017000020000"
             },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
    },
    {
      "target_scraper": "gj_upin",
      "order_id": "ORD4091843143148353",
      "search_attributes": {
        "state_code": "GJ",
        "upin": "11301025000000001"
             },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
    },
    {
      "target_scraper": "gj_upin",
      "order_id": "ORD4091843143148353",
      "search_attributes": {
        "state_code": "GJ",
        "upin": "10207025000200000"
             },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
    },
    {
      "target_scraper": "gj_upin",
      "order_id": "ORD4091843143148353",
      "search_attributes": {
        "state_code": "GJ",
        "upin": "12501079000010000"
             },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
    },
    {
      "target_scraper": "gj_upin",
      "order_id": "ORD4091843143148353",
      "search_attributes": {
        "state_code": "GJ",
        "upin": "11003033000010000"
             },
            "callback_url": callback_url,
            "auth_token": "DS5hp7XnEoKQAkv",
            "priority": "0"
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
response_filename = f"responses_dev_{current_date}.json"
response_filepath = os.path.join("/Daily_Sanity/output/", response_filename)


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
                        "district_code": payload['search_attributes'].get('district_code', None),
                        "tehsil_code": payload['search_attributes'].get('tehsil_code',None),
                        "village_code": payload['search_attributes'].get('village_code',None),
                        "survey_no": payload['search_attributes'].get('survey_no',None),
                        "survey_code": payload['search_attributes'].get('survey_code',None)
                    }
                    callback_url_mapping.append(response_data)
                    json_response.update(response_data)
            else:
                print(f"POST request failed for payload {index}, Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error while sending POST request for payload {index}: {(e)}")

print(f"Time sleep for 5 minutes started")
time.sleep(300)
print(f"Time sleep for 5 minutes completed")

ws.append(["Date", "Target Scraper", "Callback URL", "Task ID", "Status Message", "Status","State Code", "District Code",
           "Tehsil Code", "Village Code", "Survey No","survey_code","Matching Percentage", "Matching Keys", "Unmatched Keys",
           "Unmatched Data", "Exception"])

excel_file_path = "/Daily_Sanity/compare_file/Daily_Sanity_As_is-All_states.xlsx"
excel_data = pd.read_excel(excel_file_path)
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
    survey_code = api_response.get("survey_code")

    # Send the GET request to the decoded URL
    response = requests.get(callback_url)
    try:
        data = json.loads(response.text)
    except Exception as e:
        ws.append([current_date, target_scraper, callback_url, task_id, "", "Failed",state_code, district_code,tehsil_code, village_code, survey_no,survey_code,"", "", "","", str(e)])
    try:
        excel_file_path = "/Daily_Sanity/compare_file/Daily_Sanity_As_is-All_states.xlsx"
        excel_data = pd.read_excel(excel_file_path)
        response_data = data.get("data", None)
        response_data1 = data

        webhook_value3 = response_data1
        webhook_value2 = json.dumps(webhook_value3)
        webhook_value1 = str(webhook_value2).replace(" ", "").replace("\n", "").replace("\t", "")
        webhook_value = json.loads(webhook_value1)
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

                matching_keys, unmatched_keys, matched_key_value_pairs, unmatched_key_value_pairs = compare_json_objects(webhook_value, excel_value)
                matching_percentage = None

                matching_percentage = (matching_keys / (matching_keys + unmatched_keys)) * 100

                unmatched_data = "\n".join([f'{key}: {value}' for key, value in unmatched_key_value_pairs.items()])

                print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}, Matching Keys: {matching_keys}, Unmatched Keys: {unmatched_keys}, Percentage of Matching Keys: {matching_percentage:.2f}%")

                if status_code == "SCR310" or status_code == "SCR314" or status_code == "SCR315" or status_code == "SCR316" or status_code == "SCR320" or status_code == "SCR321" or status_code == "SCR305" or status_code == "SCR326" or status_code == "SCR313" or status_code == "SCR311" or status_code == "SCR305":
                    ws.append([current_date, target_scraper, callback_url, task_id, status_message, "Failed", state_code,district_code, tehsil_code, village_code, survey_no, survey_code, matching_percentage,matching_keys, unmatched_keys, unmatched_data])
                else:
                    ws.append([current_date, target_scraper, callback_url, task_id, status_message, "Success", state_code,district_code, tehsil_code, village_code, survey_no, survey_code, matching_percentage,matching_keys, unmatched_keys, unmatched_data])

            else:
                print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, Status Message: {status_message}")

        else:
            response = requests.get(callback_url)
            print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id}, {status_message},Status Message: {eval(response.text)['status']}")
            ws.append([current_date, target_scraper, callback_url, task_id,status_message, "Failed",state_code, district_code,tehsil_code, village_code, survey_no, survey_code, matching_percentage, matching_keys, unmatched_keys,unmatched_data, eval(response.text)["status"]])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        line_number = exc_tb.tb_lineno
        print(f"Target Scraper: {target_scraper}, Callback URL: {callback_url}, Task ID: {task_id},{status_message}, Exception: {str(e)}")
        print(f"Exception in {file_name} at line {line_number}: {str(e)}")
        ws.append([current_date, target_scraper, callback_url, task_id, status_message,"Failed",state_code, district_code,tehsil_code, village_code, survey_no,survey_code, matching_percentage, matching_keys, unmatched_keys,unmatched_data, str(e)])

custom_directory = "/Daily_Sanity/output/"

excel_filename = str(custom_directory) + "responses_dev_percent_" + str(datetime.now().strftime('%Y%m%d_%H%M%S')) + ".xlsx"

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


environment = 'DEV'

data_counts = count_occurrences(excel_file_path_zulip, excel_sheet_name)
table_data = create_table(data_counts)

send_to_zulip(table_data, zulip_stream_name, zulip_topic_name, zulip_email, zulip_api_key, zulip_api_site, environment)
