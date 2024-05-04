from cryptography.fernet import Fernet
from codecs import decode

from uuid import UUID
from time import sleep
from typing import Dict
import pandas as pd
import numpy as np
from requests import post, get
from requests.exceptions import HTTPError, Timeout

fernet_key = b"XIYVYpjSs3ZY-_m201seinB34SYyYiWtyuk_s4fHm1Q="

SCRAPER_API_URL = "http://dev-scraper-api.advarisk.com:8090"


def check_automation_order_status(order_id: str, task_id: UUID) -> bool:
    order_status, order_status_description = None, None
    order_status_response = get(
        f"{SCRAPER_API_URL}/utilities/order-status/{order_id}/{task_id}", timeout=10
    )
    if order_status_response.status_code == 200:
        status_response = order_status_response.json()
        order_status = status_response.get("task_status")
        if order_status > 2:
            order_status = status_response.get("task_status")
            order_status_description = status_response.get("task_status_description")
        else:
            print(
                f" Order is in {status_response.get('task_status_description')} status. Will wait for a while and check again!"
            )
    return order_status, order_status_description


def orchestrate_order(scraper_payload) -> Dict:
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    try:
        print("Input Payload: ", scraper_payload)
        create_task_response = post(
            f"{SCRAPER_API_URL}/cersai/debtor_search/",
            json=scraper_payload,
            headers=headers,
            timeout=10,
        )
        # import pdb;pdb.set_trace()
        if create_task_response.status_code == 200:
            task_id = create_task_response.json().get("task_id")
            print("Generated Task Id is ", task_id)
            automation_order_status, description = check_automation_order_status(
                scraper_payload.get("order_id"), task_id
            )
            if automation_order_status == 3:
                return {
                    "automation_order_status": True,
                    "response_message": create_task_response.text,
                    "task_id": task_id,
                }
            else:
                print(f"order execution failed --> {description}")
                return {
                    "automation_order_status": False,
                    "response_message": f"Order failed --> {description}",
                    "task_id": task_id,
                }
        elif create_task_response.status_code == 400:
            return {
                "automation_order_status": False,
                "response_message": "problem in payload",
            }
        elif create_task_response.status_code == 500:
            return {
                "automation_order_status": False,
                "response_message": "problem at Scraper API",
                "task_id": task_id,
            }
        else:
            return {
                "automation_order_status": False,
                "response_message": f"Unknown response from Scraper API --> {create_task_response.text}",
            }
    except (HTTPError, TimeoutError) as e:
        return {
            "automation_order_status": False,
            "response_message": f"Not able to connect to Scraper API --> {e}",
        }


def encrypt_string(input_string: str):
    encryptor = Fernet(fernet_key)
    return decode(encryptor.encrypt(input_string.encode("utf-8")))


def create_debtor_based_search_payload(
    order_id,
    pan_number,
    type_of_debtor,
    payment_method,
    upi_id,
    callback,
    auth_token="veci7UdKRTeWBEu",
):
    """Function to create scraper payload for cersai debtor based search"""
    scraper_payload = {
        "order_id": order_id,
        "search_data": {"debtor_type": type_of_debtor, "pan_number": pan_number},
        "payment_method": payment_method,
        "callback_url": callback,
        "authentication_token": auth_token,
        "user_email_id": "santhosh@advarisk.com",
    }
    if upi_id:
        encrypted_upi_id = encrypt_string(upi_id)
        scraper_payload.update({"upi_id": encrypted_upi_id})
    # print(encrypted_upi_id)

    return scraper_payload


def trigger_scraper_api(
    input_csv_file_path,
    pan_number_col_name,
    debtor_type_col_name,
    payment_method_col_name,
    upi_id_col_name,
):
    retrigger_order_df = pd.read_csv(input_csv_file_path)
    # retrigger_order_df = retrigger_order_df.where(pd.notnull(retrigger_order_df), None)
    retrigger_order_df = retrigger_order_df.replace(np.nan, None)

    # retrigger_order_df = retrigger_order_df[0:1]
    print(retrigger_order_df.head())
    pan_number_list = retrigger_order_df[pan_number_col_name].to_list()
    debtor_type_list = retrigger_order_df[debtor_type_col_name].to_list()
    payment_method_list = retrigger_order_df[payment_method_col_name].to_list()
    upi_id_list = retrigger_order_df[upi_id_col_name].to_list()
    order_id_list = []
    task_id_list = []
    response = None
    print(pan_number_list)
    for index in range(len(pan_number_list)):
        order_id = f"cersai_debtor_{index+1}"
        order_id_list.append(order_id)
        payment_method = payment_method_list[index]
        request_payload = create_debtor_based_search_payload(
            pan_number=pan_number_list[index],
            type_of_debtor=debtor_type_list[index],
            payment_method=payment_method,
            upi_id=upi_id_list[index],
            callback=f"https://webhook.advarisk.com/webhook/11/{order_id}",
            order_id=order_id,
        )
        response = orchestrate_order(request_payload)
        print(response)
        task_id_list.append(response.get("task_id"))
    retrigger_order_df["order_id_list"] = order_id_list
    retrigger_order_df["task_id_list"] = task_id_list
    retrigger_order_df.to_csv(
        input_csv_file_path.replace(".csv", "_result.csv"), index=False
    )
    return response


# ----------------------------------------Call Function-------------------------------
input_file_path = "/home/maheshshingade/stress_testing cersai dbs/given_ord_id.csv"
result = trigger_scraper_api(
    input_file_path, "PAN", "BORROWER_TYPE", "PAYMENT_METHOD", "UPI_ID"
)
print(result)
# print(encrypt_string("7448064065@ybl"))
