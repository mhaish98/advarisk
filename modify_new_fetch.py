from pymongo import MongoClient
import pandas as pd

MONGO_DB_URI = (
    "mongodb://scraperProd:mqCCwVtv4dw3ec76@mongo-db.advarisk.com:27110/?ssl=true"
)
DATABASE = "Land_Records"
COLLECTION = "parsed_data"


def mongo_handler(database, collection):
    try:
        mongo = MongoClient(MONGO_DB_URI)
    except Exception as e:
        print(e)
    mongo_connection = mongo[database][collection]
    return mongo_connection


def get_document_link(task_id):
    if task_id:
        mongo_connection = mongo_handler(DATABASE, COLLECTION)
        result = mongo_connection.find_one({"order_id": task_id})
        # print(result)
        if result:
            err_msg = result.get("errmsg", {}) or result.get("status", {})
            pan_number = result.get("pan", {})
            zip_link = result.get("cloud_path", {})
            return pan_number, zip_link, err_msg
        else:
            return {}, {}, "response_not_found",
    else:
        return {}, {}, "task_id_not_generated",


def get_task_status(input_csv_file_path, task_id_col_name):
    retrigger_order_df = pd.read_csv(input_csv_file_path)
    # pan_number_list = retrigger_order_df[pan_number_col_name].to_list()S
    task_id_list = retrigger_order_df[task_id_col_name].to_list()
    found_pan_number_list = []
    zip_folder_link = []
    error_messeges = []
    for index in range(len(task_id_list)):
        pan_number, zip_link, err_msg = get_document_link(task_id_list[index])

        found_pan_number_list.append(pan_number)
        zip_folder_link.append(zip_link)
        error_messeges.append(err_msg)
    
    retrigger_order_df["found_pan_number"] = found_pan_number_list
    retrigger_order_df["zip_folder_link"] = zip_folder_link
    retrigger_order_df["Status_messege"] = error_messeges

    retrigger_order_df.to_csv(
        input_csv_file_path.replace(".csv", "_status.csv"), index=False
    )
    return True


# ---------------------------Call Functions--------------------------
input_file_path = "/home/maheshshingade/stress_testing cersai dbs/given_ord_id_result.csv"

result = get_task_status(input_file_path, "task_id_list")
print(result)
