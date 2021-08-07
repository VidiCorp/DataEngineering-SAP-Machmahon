import os
import sys
import datetime
import logging
import requests
from requests.auth import HTTPBasicAuth
import xmltodict

import pandas as pd
import numpy as np

from utils import send_df_to_sql
from utils import create_pd

logging.basicConfig(level="INFO")
logger = logging.getLogger('WORK_ORDERS')

DEFAULT_INT_VALUE = 0
DEFAULT_STRING_VALUE = ''

user = os.getenv("SAP_USER", "SAP_USERNAME")
password = os.getenv("PASSWORD", "SAP_PASSWORD")

records = []
wo_columns = ["ID","MaintenanceOrder", "MaintenanceOrderType", "MaintenanceOrderDesc", "MaintPriority",
              "MaintPriorityType", "Equipment", "WBSElementInternalID", "LastChangeDate", "LastChangeTime",
              "PlannedStartDate", "PlannedStartTime", "PlannedEndDate", "PlannedEndTime", "ScheduledBasicStartDate",
              "ScheduledBasicStartTime", "ScheduledBasicEndDate", "ScheduledBasicEndTime", "ActualStartDate",
              "ActualStartTime", "ConfirmedEndDate", "ConfirmedEndTime", "MaintOrderReferenceDate", "WorkCenterTypeCode",
              "MainWorkCenter", "MainWorkCenterPlant", "MaintenancePlant", "MaintenancePlannerGroupName",
              "OrderTypeName", "MaintenanceActivityTypeName","MaintenancePlannerGroup","CompanyCode",
              "IsMarkedForDeletion", "NmbrOfMaintOrdsNotToBeExecuted", "NumberOfCompletedMaintOrders",
              "CostCenter", "CostCenterName", "Department", "ValidityEndDate", "ValidityStartDate", "LastChangeDateTime",
              "MaintenanceProcessingPhase", "TechnicalObjectTypeDesc", "MaintenanceActivityType", "CreationDate",
              "OrderRuntimeDuration", "OrderDurationUnit"]

    
def get_work_order_data(skiptoken):
    logger.info("Processing data from: {}".format(skiptoken))
    work_order_url = os.getenv("WORK_ORDER_URL",
                               "https://id.api.s4hana.ondemand.com/sap/opu/odata/"
                               "sap/YY1_WORKORDER_CDS/YY1_WorkOrder?$skiptoken={}".format(skiptoken))

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


def prepare_work_order_data(wo_response):
    try:
        json_data = xmltodict.parse(wo_response)
        logger.info("Prepared JSON Content: {}".format(json_data))
        logger.info("Total entries: {}".format(len(json_data['feed']['entry'])))

        for e in json_data["feed"]["entry"]:
            keys = e['content']['m:properties'].keys()
            logger.debug("Keys: {}".format(keys))
            ID = str(e['content']['m:properties']['d:ID'])[3:]
            MaintenanceOrder = str(e['content']['m:properties']['d:MaintenanceOrder'])
            MaintenanceOrderType = str(e['content']['m:properties']['d:MaintenanceOrderType'])
            MaintenanceOrderDesc = str(e['content']['m:properties']['d:MaintenanceOrderDesc'])
            MaintPriorityType = str(e['content']['m:properties']['d:MaintPriorityType'])
            if e['content']['m:properties']['d:MaintPriority'] is None:
                MaintPriority = DEFAULT_INT_VALUE
            else:
                MaintPriority = int(e['content']['m:properties']['d:MaintPriority'])
            Equipment = str(e['content']['m:properties']['d:Equipment'])
            WBSElementInternalID = str(e['content']['m:properties']['d:WBSElementInternalID'])

            if "null" in str(e['content']['m:properties']['d:LastChangeDate']):
                LastChangeDate = np.nan
            else:
                LastChangeDate = str(e['content']['m:properties']['d:LastChangeDate'])

            if "null" in str(e['content']['m:properties']['d:LastChangeTime']):
                LastChangeTime = np.nan
            else:
                LastChangeTime = str(e['content']['m:properties']['d:LastChangeTime'])

            if "null" in str(e['content']['m:properties']['d:PlannedStartDate']):
                PlannedStartDate = np.nan
            else:
                PlannedStartDate = str(e['content']['m:properties']['d:PlannedStartDate'])

            # if "null" in str(e['content']['m:properties']['d:PlannedStartTIme']):
            #     PlannedStartTime = np.nan
            # else:
            # if "d:PlannedStartTime" not in keys or "null" in str(e['content']['m:properties']['d:PlannedStartTIme']):
            #     logger.info("Assigning null to d:PlannedStartTime")
            #     PlannedStartTime = np.nan
            # else:
            PlannedStartTime = str(e['content']['m:properties']['d:PlannedStartTime'])

            if "null" in str(e['content']['m:properties']['d:PlannedEndDate']):
                PlannedEndDate = np.nan
            else:
                PlannedEndDate = str(e['content']['m:properties']['d:PlannedEndDate'])

            if "null" in str(e['content']['m:properties']['d:PlannedEndTime']):
                PlannedEndTime = np.nan
            else:
                PlannedEndTime = str(e['content']['m:properties']['d:PlannedEndTime'])

            if "null" in str(e['content']['m:properties']['d:ScheduledBasicStartDate']):
                ScheduledBasicStartDate = np.nan
            else:
                ScheduledBasicStartDate = str(e['content']['m:properties']['d:ScheduledBasicStartDate'])

            if "null" in str(e['content']['m:properties']['d:ScheduledBasicStartTime']):
                ScheduledBasicStartTime = np.nan
            else:
                ScheduledBasicStartTime = str(e['content']['m:properties']['d:ScheduledBasicStartTime'])

            if "null" in str(e['content']['m:properties']['d:ScheduledBasicEndDate']):
                ScheduledBasicEndDate = np.nan
            else:
                ScheduledBasicEndDate = str(e['content']['m:properties']['d:ScheduledBasicEndDate'])

            if "null" in str(e['content']['m:properties']['d:ScheduledBasicEndTime']):
                ScheduledBasicEndTime = np.nan
            else:
                ScheduledBasicEndTime = str(e['content']['m:properties']['d:ScheduledBasicEndTime'])

            if "null" in str(e['content']['m:properties']['d:ActualStartDate']):
                ActualStartDate = np.nan
            else:
                ActualStartDate = str(e['content']['m:properties']['d:ActualStartDate'])

            if "null" in str(e['content']['m:properties']['d:ActualStartTime']):
                ActualStartTime = np.nan
            else:
                ActualStartTime = str(e['content']['m:properties']['d:ActualStartTime'])

            if "null" in str(e['content']['m:properties']['d:ConfirmedEndDate']):
                ConfirmedEndDate = np.nan
            else:
                ConfirmedEndDate = str(e['content']['m:properties']['d:ConfirmedEndDate'])

            # if "null" in str(e['content']['m:properties']['d:ConfirmedEndTIme']):
            #     ConfirmedEndTime = np.nan
            # else:
            ConfirmedEndTime = str(e['content']['m:properties']['d:ConfirmedEndTime'])

            if "null" in str(e['content']['m:properties']['d:MaintOrderReferenceDate']):
                MaintOrderReferenceDate = np.nan
            else:
                MaintOrderReferenceDate = str(e['content']['m:properties']['d:MaintOrderReferenceDate'])

            WorkCenterTypeCode = str(e['content']['m:properties']['d:WorkCenterTypeCode'])
            MainWorkCenter = str(e['content']['m:properties']['d:MainWorkCenter'])
            MainWorkCenterPlant = str(e['content']['m:properties']['d:MainWorkCenterPlant'])
            MaintenancePlant = str(e['content']['m:properties']['d:MaintenancePlant'])
            MaintenancePlannerGroupName = str(e['content']['m:properties']['d:MaintenancePlannerGroupName'])
            OrderTypeName = str(e['content']['m:properties']['d:OrderTypeName'])
            MaintenanceActivityTypeName = str(e['content']['m:properties']['d:MaintenanceActivityTypeName'])
            MaintenancePlannerGroup = str(e['content']['m:properties']['d:MaintenancePlannerGroup'])
            CompanyCode = str(e['content']['m:properties']['d:CompanyCode'])
            IsMarkedForDeletion = str(e['content']['m:properties']['d:IsMarkedForDeletion'])
            NmbrOfMaintOrdsNotToBeExecuted = int(e['content']['m:properties']['d:NmbrOfMaintOrdsNotToBeExecuted'])
            NumberOfCompletedMaintOrders = int(e['content']['m:properties']['d:NumberOfCompletedMaintOrders'])
            CostCenter = str(e['content']['m:properties']['d:CostCenter'])
            CostCenterName = str(e['content']['m:properties']['d:CostCenterName'])
            Department = str(e['content']['m:properties']['d:Department'])

            if "null" in str(e['content']['m:properties']['d:ValidityEndDate']):
                ValidityEndDate = np.nan
            else:
                ValidityEndDate = str(e['content']['m:properties']['d:ValidityEndDate'])

            if "null" in str(e['content']['m:properties']['d:ValidityStartDate']):
                ValidityStartDate = np.nan
            else:
                ValidityStartDate = str(e['content']['m:properties']['d:ValidityStartDate'])

            if "null" in str(e['content']['m:properties']['d:LastChangeDateTime']):
                LastChangeDateTime = np.nan
            else:
                LastChangeDateTime = str(e['content']['m:properties']['d:LastChangeDateTime'])

            MaintenanceProcessingPhase = str(e['content']['m:properties']['d:MaintenanceProcessingPhase'])
            TechnicalObjectTypeDesc = str(e['content']['m:properties']['d:TechnicalObjectTypeDesc'])
            MaintenanceActivityType = str(e['content']['m:properties']['d:MaintenanceActivityType'])

            if "null" in str(e['content']['m:properties']['d:CreationDate']):
                CreationDate = np.nan
            else:
                CreationDate = str(e['content']['m:properties']['d:CreationDate'])

            OrderRuntimeDuration = str(e['content']['m:properties']['d:OrderRuntimeDuration'])
            OrderDurationUnit = str(e['content']['m:properties']['d:OrderDurationUnit'])

            record_tuple = (ID, MaintenanceOrder, MaintenanceOrderType, MaintenanceOrderDesc, MaintPriority,
                            MaintPriorityType, Equipment, WBSElementInternalID, LastChangeDate, LastChangeTime,
                            PlannedStartDate, PlannedStartTime, PlannedEndDate, PlannedEndTime,ScheduledBasicStartDate,
                            ScheduledBasicStartTime, ScheduledBasicEndDate, ScheduledBasicEndTime, ActualStartDate,
                            ActualStartTime, ConfirmedEndDate, ConfirmedEndTime, MaintOrderReferenceDate, WorkCenterTypeCode,
                            MainWorkCenter, MainWorkCenterPlant, MaintenancePlant, MaintenancePlannerGroupName,
                            OrderTypeName, MaintenanceActivityTypeName, MaintenancePlannerGroup, CompanyCode,
                            IsMarkedForDeletion, NmbrOfMaintOrdsNotToBeExecuted, NumberOfCompletedMaintOrders,
                            CostCenter, CostCenterName, Department, ValidityEndDate, ValidityStartDate, LastChangeDateTime,
                            MaintenanceProcessingPhase, TechnicalObjectTypeDesc, MaintenanceActivityType, CreationDate,
                            OrderRuntimeDuration, OrderDurationUnit
                            )

            records.append(record_tuple)

    except Exception as e:
        logger.error("Exception raised while preparing data: {}".format(e))
        return None


def prepare_wo_data_and_pd():
    next = True
    skiptoken = 0
    while next:
        result = get_work_order_data(skiptoken)
        prepare_work_order_data(result)
        json = xmltodict.parse(result)
        if "link" in json['feed'].keys():
            if len(json['feed']['link']) == 2:
                skiptoken = skiptoken + 1000
            else:
                next = False

    if len(records) == 0:
        return None
    df = create_pd(records, wo_columns)
    logger.info("Current time: {}".format(datetime.datetime.now()))
    df['lastupdatedtime'] = str(datetime.datetime.now())

    return df


def prepare_wo_data_and_send_to_sql():
    table_name = "macmahon_work_orders"
    data = prepare_wo_data_and_pd()
    logger.info("Sending WO data to SQL: {}".format(data))
    if isinstance(data, pd.DataFrame):
        send_df_to_sql(data, table_name)

    return data


if __name__ == '__main__':
    records_df = prepare_wo_data_and_send_to_sql()
    if records_df is None:
        logger.error("Error processing records, did not receive a dataframe output")
        sys.exit(1)
