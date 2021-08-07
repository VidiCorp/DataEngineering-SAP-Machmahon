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
logger = logging.getLogger('PURCHASE_ORDER')

user = os.getenv("SAP_USER", "SAP_USERNAME")
password = os.getenv("PASSWORD", "SAP_PASSWORD")
records = []
po_columns = ["PurchaseOrder", "PurchaseOrderItem", "AccountAssignmentNumber", "IsDeleted", "PurchaseOrderQuantityUnit",
               "Quantity", "MultipleAcctAssgmtDistrPercent", "DocumentCurrency", "PurgDocNetAmount",
               "GLAccount", "SalesOrderItem", "SalesOrderScheduleLine", "OrderID", "ProfitCenter",
               "WBSElementInternalID", "WBSElement", "WBSElementExternalID", "FunctionalArea",
               "SettlementReferenceDate"
             ]


def get_purchase_order_data(skiptoken=0):
    logger.info("Fetching records from: {}".format(skiptoken))
    work_order_url = os.getenv("PURCHASE_ORDER_URL",
                               "https://id.api.s4hana.ondemand.com/sap/opu/odata/sap/"
                               "API_PURCHASEORDER_PROCESS_SRV/A_PurOrdAccountAssignment"
                               "?$skiptoken={}").format(skiptoken)

    logger.debug("URL: {}, Username: {} , Password: {}".format(work_order_url, user, password))

    # Making a get request
    response = requests.get(work_order_url, auth=HTTPBasicAuth(user, password))
    response.raw.decode_content = True

    if response.status_code != 200:
        logger.error("Received status code: {}, Content {}".format(response.status_code, response.content))
        return None

    logger.info("Response status: {}".format(response.status_code))
    logger.info("Response content: {}".format(response.content))

    return response.content


def prepare_purchase_order_data(po_response):
    try:
        json_data = xmltodict.parse(po_response)
        logger.info("Prepared JSON Content: {}".format(json_data))
        logger.info("Total entries: {}".format(len(json_data['feed']['entry'])))

        for e in json_data["feed"]["entry"]:
            try:
                keys = e['content']['m:properties'].keys()
                record = e['content']['m:properties']
                logger.debug("Keys: {}, record: {}".format(keys, record))

                PurchaseOrder = record["d:PurchaseOrder"]
                PurchaseOrderItem = str(record["d:PurchaseOrderItem"])
                AccountAssignmentNumber = str(record["d:AccountAssignmentNumber"])
                IsDeleted = str(record["d:IsDeleted"])
                PurchaseOrderQuantityUnit = str(record["d:PurchaseOrderQuantityUnit"])
                Quantity = str(record["d:Quantity"])
                MultipleAcctAssgmtDistrPercent = str(record["d:MultipleAcctAssgmtDistrPercent"])
                DocumentCurrency = str(record["d:DocumentCurrency"])
                PurgDocNetAmount = record["d:PurgDocNetAmount"]
                GLAccount = record["d:GLAccount"]
                SalesOrderItem = record["d:SalesOrderItem"]
                SalesOrderScheduleLine = str(record["d:SalesOrderScheduleLine"])
                OrderID = str(record["d:OrderID"])
                ProfitCenter = str(record["d:ProfitCenter"])
                WBSElementInternalID = str(record["d:WBSElementInternalID"])
                WBSElement = str(record["d:WBSElement"])
                WBSElementExternalID = str(record["d:WBSElementExternalID"])
                FunctionalArea = str(record["d:FunctionalArea"])
                if "null" in str(record['d:SettlementReferenceDate']):
                    SettlementReferenceDate = np.nan
                else:
                    SettlementReferenceDate = str(record['d:SettlementReferenceDate'])

                record = (PurchaseOrder, PurchaseOrderItem, AccountAssignmentNumber, IsDeleted, PurchaseOrderQuantityUnit, Quantity,
                          MultipleAcctAssgmtDistrPercent, DocumentCurrency, PurgDocNetAmount, GLAccount, SalesOrderItem,
                          SalesOrderScheduleLine, OrderID, ProfitCenter, WBSElementInternalID, WBSElement,
                          WBSElementExternalID, FunctionalArea, SettlementReferenceDate
                          )

                records.append(record)

            except Exception as e:
                logger.error("Error parsing record: {}".format(e))

    except Exception as e:
        logger.error("Error parsing purchase order data: {}".format(e))


def prepare_po_data_and_pd():
    next = True
    skiptoken = 0
    while next:
        result = get_purchase_order_data(skiptoken)
        prepare_purchase_order_data(result)
        json = xmltodict.parse(result)
        if "link" in json['feed'].keys():
            if len(json['feed']['link']) == 2:
                skiptoken = skiptoken + 1000
            else:
                next = False

    logger.info("Total number of columns: {}".format(len(po_columns)))
    df = create_pd(records, po_columns)
    logger.info("Current time: {}".format(datetime.datetime.now()))
    df['lastupdatedtime'] = str(datetime.datetime.now())

    return df


def prepare_mo_data_and_send_to_sql():
    table_name = "macmahon_purchase_orders"
    data = prepare_po_data_and_pd()
    logger.debug("Sending PO data to SQL: {}".format(data))
    if isinstance(data, pd.DataFrame):
        send_df_to_sql(data, table_name)

    return data


if __name__ == '__main__':
    prepare_mo_data_and_send_to_sql()

