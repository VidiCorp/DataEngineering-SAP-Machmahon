import os
import sys
import logging
import requests
import datetime
from requests.auth import HTTPBasicAuth
import xmltodict

import pandas as pd

from utils import send_df_to_sql
from utils import create_pd

DEFAULT_INT_VALUE = 0
DEFAULT_STRING_VALUE = ''

user = os.getenv("SAP_USER", "SAP_USERNAME")
password = os.getenv("PASSWORD", "SAP_PASSWORD")

logging.basicConfig(level="INFO")
logger = logging.getLogger('Maintenance Order')

records = []
columns = ["MaintenanceOrder", "MaintOrderOperationCounter", "MaintOrderRoutingNumber", "FunctionalLocation",
           "Equipment", "MaintenanceActivityType", "MaintenancePlannerGroup", "MaintenancePlanningPlant",
           "MaintenanceOrderType", "MaintenancePlant", "MaintObjectLocAcctAssgmtNmbr",
           "MaintenanceOrderOperation", "OperationPersonResponsible", "OperationControlKey",
           "OperationDescription", "WorkCenter", "WorkCenterPlant", "OperationPlannedWork",
           "OperationPlannedWorkUnit", "ConfirmationTotalQuantity", "OperationQuantity", "CreationDate",
           "LastChangeDateTime", "MaintOrderConfirmation", "MaintOrderOperationInternalID",
           "SuperiorOperationInternalID", "OperationWorkCenterInternalID", "OperationWorkCenterTypeCode"
           ]


def get_maintenance_order_data(skiptoken):
    work_order_url = os.getenv("MAINTENANCE_ORDER_URL",
                               "https://id.api.s4hana.ondemand.com/sap/opu/odata/sap/"
                               "YY1_MOOPERATIONDATA_CDS/YY1_MOOperationData?$skiptoken={}".format(skiptoken))

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


def prepare_maintenance_order_data(mo_response):
    try:
        json_data = xmltodict.parse(mo_response)
        logger.info("Prepared JSON Content: {}".format(json_data))
        logger.info("Total entries: {}".format(len(json_data['feed']['entry'])))

        for e in json_data["feed"]["entry"]:
            try:
                keys = e['content']['m:properties'].keys()
                record = e['content']['m:properties']
                logger.debug("Keys: {}, record: {}".format(keys, record))

                MaintenanceOrder = str(record["d:MaintenanceOrder"])
                MaintOrderOperationCounter = str(record["d:MaintOrderOperationCounter"])
                MaintOrderRoutingNumber = str(record["d:MaintOrderRoutingNumber"])
                FunctionalLocation = str(record["d:FunctionalLocation"])
                Equipment = str(record["d:Equipment"])
                MaintenanceActivityType = str(record["d:MaintenanceActivityType"])
                MaintenancePlannerGroup = str(record["d:MaintenancePlannerGroup"])
                MaintenancePlanningPlant = str(record["d:MaintenancePlanningPlant"])
                MaintenanceOrderType = str(record["d:MaintenanceOrderType"])
                MaintenancePlant = str(record["d:MaintenancePlant"])
                MaintObjectLocAcctAssgmtNmbr = str(record["d:MaintObjectLocAcctAssgmtNmbr"])
                MaintenanceOrderOperation = str(record["d:MaintenanceOrderOperation"])
                OperationPersonResponsible = str(record["d:OperationPersonResponsible"])
                OperationControlKey = str(record["d:OperationControlKey"])
                OperationDescription = str(record["d:OperationDescription"])
                WorkCenter = str(record["d:WorkCenter"])
                WorkCenterPlant = str(record["d:WorkCenterPlant"])
                OperationPlannedWork = str(record["d:OperationPlannedWork"])
                OperationPlannedWorkUnit = str(record["d:OperationPlannedWorkUnit"])
                ConfirmationTotalQuantity = record["d:ConfirmationTotalQuantity"]
                OperationQuantity = record["d:OperationQuantity"]
                CreationDate = str(record["d:CreationDate"])
                LastChangeDateTime = str(record["d:LastChangeDateTime"])
                MaintOrderConfirmation = str(record["d:MaintOrderConfirmation"])
                MaintOrderOperationInternalID = str(record["d:MaintOrderOperationInternalID"])
                SuperiorOperationInternalID = str(record["d:SuperiorOperationInternalID"])
                OperationWorkCenterInternalID = str(record["d:OperationWorkCenterInternalID"])
                OperationWorkCenterTypeCode = str(record["d:OperationWorkCenterTypeCode"])

                record = (MaintenanceOrder, MaintOrderOperationCounter, MaintOrderRoutingNumber, FunctionalLocation,
                          Equipment, MaintenanceActivityType, MaintenancePlannerGroup, MaintenancePlanningPlant,
                          MaintenanceOrderType, MaintenancePlant, MaintObjectLocAcctAssgmtNmbr,
                          MaintenanceOrderOperation, OperationPersonResponsible, OperationControlKey,
                          OperationDescription, WorkCenter, WorkCenterPlant, OperationPlannedWork,
                          OperationPlannedWorkUnit, ConfirmationTotalQuantity, OperationQuantity,
                          CreationDate, LastChangeDateTime, MaintOrderConfirmation, MaintOrderOperationInternalID,
                          SuperiorOperationInternalID, OperationWorkCenterInternalID, OperationWorkCenterTypeCode)

                records.append(record)
            except Exception as e:
                logger.error("Error parsing data: {}".format(e))
                pass

    except Exception as e:
        logger.error("Exception raised while processing maintenance order records: {}".format(e))
        return None


def prepare_mo_data_and_pd():
    next = True
    skiptoken = 0
    while next:
        result = get_maintenance_order_data()
        prepare_maintenance_order_data(result)
        json = xmltodict.parse(result)
        if "link" in json['feed'].keys():
            if len(json['feed']['link']) == 2:
                skiptoken = skiptoken + 1000
            else:
                next = False

    logger.info("Total number of records: {}, columns: {}".format(len(records), len(columns)))
    df = create_pd(records, columns)
    logger.info("Current time: {}".format(datetime.datetime.now()))
    df['lastupdatedtime'] = str(datetime.datetime.now())

    return df


def prepare_mo_data_and_send_to_sql():
    table_name = "macmahon_maintenance_orders"
    data = prepare_mo_data_and_pd()
    logger.info("Sending MO data to SQL: {}".format(data))
    if isinstance(data, pd.DataFrame):
        send_df_to_sql(data, table_name)

    return data


if __name__ == '__main__':
    prepare_mo_data_and_send_to_sql()
