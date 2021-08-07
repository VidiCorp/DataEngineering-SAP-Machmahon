import os
import logging
import requests
import datetime
from requests.auth import HTTPBasicAuth
import xmltodict

import pandas as pd
import numpy as np

from utils import send_df_to_sql
from utils import create_pd

DEFAULT_INT_VALUE = 0
DEFAULT_STRING_VALUE = ''

logging.basicConfig(level="INFO")
logger = logging.getLogger('DOCUMENT_HEADER')

user = os.getenv("SAP_USER", "SAP_USERNAME")
password = os.getenv("PASSWORD", "SAP_PASSWORD")

records = []
document_header_columns = ["Reservation", "OrderID", "GoodsMovementType", "CostCenter", "GoodsRecipientName",
                           "ReservationDate", "IsCheckedAgainstFactoryCal", "Customer", "WBSElement",
                           "ControllingArea", "SalesOrder", "SalesOrderItem", "SalesOrderScheduleLine", "AssetNumber",
                           "AssetSubNumber", "NetworkNumberForAcctAssgmt", "IssuingOrReceivingPlant",
                           "IssuingOrReceivingStorageLoc"]


def get_document_record_header(skiptoken):
    logger.info("Fetching records from: {}".format(skiptoken))
    document_header_url = os.getenv("DOCUMENT_HEADER_URL", "https://id.api.s4hana.ondemand.com/sap/opu/"
                                                           "odata/sap/API_RESERVATION_DOCUMENT_SRV/"
                                                           "A_ReservationDocumentHeader?$skiptoken={}").format(skiptoken)

    logger.debug("URL: {}, Username: {} , Password: {}".format(document_header_url, user, password))

    # Making a get request
    response = requests.get(document_header_url, auth=HTTPBasicAuth(user, password))
    response.raw.decode_content = True

    if response.status_code != 200:
        logger.error("Received status code: {}, Content {}".format(response.status_code, response.content))
        return None

    logger.info("Response status: {}".format(response.status_code))
    logger.info("Response content: {}".format(response.content))

    return response.content


def prepare_document_header(dh_response):

    try:
        json_data = xmltodict.parse(dh_response)
        logger.info("Prepared JSON Content: {}".format(json_data))
        logger.info("Total entries: {}".format(len(json_data['feed']['entry'])))

        for e in json_data["feed"]["entry"]:
            try:
                record = e['content']['m:properties']
                keys = e['content']['m:properties'].keys()
                logger.debug("Keys: {}, record: {}".format(keys, record))

                Reservation = str(record["d:Reservation"])
                OrderID = str(record["d:OrderID"])
                GoodsMovementType = str(record["d:GoodsMovementType"])
                CostCenter = str(record["d:CostCenter"])
                GoodsRecipientName = str(record["d:GoodsRecipientName"])
                if "null" in str(record["d:ReservationDate"]):
                    ReservationDate = np.nan
                else:
                    ReservationDate = str(record["d:ReservationDate"])

                IsCheckedAgainstFactoryCal = str(record["d:IsCheckedAgainstFactoryCal"])
                Customer = str(record["d:Customer"])
                WBSElement = str(record["d:WBSElement"])
                ControllingArea = str(record["d:ControllingArea"])
                SalesOrder = str(record["d:SalesOrder"])
                SalesOrderItem = str(record["d:SalesOrderItem"])
                SalesOrderScheduleLine = str(record["d:SalesOrderScheduleLine"])
                AssetNumber = str(record["d:AssetNumber"])
                AssetSubNumber = str(record["d:AssetSubNumber"])
                NetworkNumberForAcctAssgmt = str(record["d:NetworkNumberForAcctAssgmt"])
                IssuingOrReceivingPlant = str(record["d:IssuingOrReceivingPlant"])
                IssuingOrReceivingStorageLoc = str(record["d:IssuingOrReceivingStorageLoc"])

                record_tuple = (Reservation, OrderID, GoodsMovementType, CostCenter, GoodsRecipientName,
                                ReservationDate, IsCheckedAgainstFactoryCal, Customer, WBSElement, ControllingArea,
                                SalesOrder, SalesOrderItem, SalesOrderScheduleLine, AssetNumber, AssetSubNumber,
                                NetworkNumberForAcctAssgmt, IssuingOrReceivingPlant, IssuingOrReceivingStorageLoc)

                records.append(record_tuple)

            except Exception as e:
                logger.error("Exception raised while processing record: {}, message: {}".format(record, e))

    except Exception as e:
        logger.error("Error preparing document header records: {}".format(e))
        return None


def prepare_dh_data_and_pd():
    next = True
    skiptoken = 0
    while next:
        result = get_document_record_header(skiptoken)
        prepare_document_header(result)
        json = xmltodict.parse(result)
        if "link" in json['feed'].keys():
            if len(json['feed']['link']) == 2:
                skiptoken = skiptoken + 1000
            else:
                next = False

    logger.info("Total number of columns: {}".format(len(document_header_columns)))
    df = create_pd(records, document_header_columns)
    logger.info("Current time: {}".format(datetime.datetime.now()))
    df['lastupdatedtime'] = str(datetime.datetime.now())

    return df


def prepare_dh_data_and_send_to_sql():
    table_name = "macmahon_document_headers"
    data = prepare_dh_data_and_pd()
    logger.debug("Sending PO data to SQL: {}".format(data))
    if isinstance(data, pd.DataFrame):
        send_df_to_sql(data, table_name)

    return data


if __name__ == '__main__':
    prepare_dh_data_and_send_to_sql()
