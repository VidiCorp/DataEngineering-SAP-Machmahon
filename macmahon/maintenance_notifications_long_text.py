import os
import logging

import numpy as np
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
logger = logging.getLogger('MaintenanceNotificationsLongText')

records = []
columns = ['MaintenanceNotification', 'MaintNotifInternalID', 'NotificationText', 'MaintPriority',
           'NotificationType', 'NotifProcessingPhase', 'NotifProcessingPhaseDesc', 'MaintPriorityDesc',
           'CreationDate', 'LastChangeTime', 'LastChangeDate', 'LastChangeDateTime', 'CreationTime',
           'ReportedByUser', 'ReporterFullName', 'PersonResponsible', 'MalfunctionEffect',
           'MalfunctionEffectText', 'MalfunctionStartDate', 'MalfunctionStartTime', 'MalfunctionEndDate',
           'MalfunctionEndTime', 'MaintNotificationCatalog', 'MaintNotificationCode', 'MaintNotificationCodeGroup',
           'CatalogProfile', 'NotificationCreationDate', 'NotificationCreationTime', 'NotificationTimeZone',
           'RequiredStartDate', 'RequiredStartTime', 'RequiredEndDate', 'RequiredEndTime',
           'LatestAcceptableCompletionDate', 'MaintenanceObjectIsDown', 'MaintNotificationLongText',
           'MaintNotifLongTextForEdit', 'TechnicalObject', 'TechObjIsEquipOrFuncnlLoc', 'TechnicalObjectLabel',
           'MaintenancePlanningPlant', 'MaintenancePlannerGroup', 'PlantSection', 'ABCIndicator',
           'SuperiorTechnicalObject', 'SuperiorTechnicalObjectName', 'SuperiorObjIsEquipOrFuncnlLoc',
           'SuperiorTechnicalObjectLabel', 'ManufacturerPartTypeName', 'TechObjIsEquipOrFuncnlLocDesc',
           'FunctionalLocation', 'TechnicalObjectDescription', 'AssetLocation', 'LocationName', 'BusinessArea',
           'CompanyCode', 'TechnicalObjectCategory', 'TechnicalObjectType', 'MainWorkCenterPlant', 'MainWorkCenter',
           'PlantName', 'MaintenancePlannerGroupName', 'MaintenancePlant', 'LocationDescription', 'MainWorkCenterText',
           'MainWorkCenterPlantName', 'MaintenancePlantName', 'PlantSectionPersonRespName', 'ABCIndicatorDesc',
           'PersonResponsibleName', 'MaintenanceOrder', 'MaintenanceOrderType', 'ConcatenatedActiveSystStsName',
           'MaintenanceActivityType', 'MaintObjDowntimeDurationUnit', 'MaintObjectDowntimeDuration', 'MaintenancePlan',
           'MaintenanceItem', 'TaskListGroup', 'TaskListGroupCounter', 'MaintenancePlanCallNumber',
           'MaintenanceTaskListType', 'NotificationReferenceDate', 'NotificationReferenceTime',
           'NotificationCompletionDate', 'CompletionTime', 'AssetRoom', 'MaintNotifExtReferenceNumber',
           'MaintNotifRejectionReasonCode', 'MaintNotifRejectionRsnCodeTxt', 'MaintNotifDetectionCodeText',
           'MaintNotifDetectionCodeGrpTxt', 'MaintNotifProcessPhaseCode', 'MaintNotifProcessSubPhaseCode',
           'EAMProcessPhaseCodeDesc', 'EAMProcessSubPhaseCodeDesc']


def get_maintenance_notifications_long_text_data(skiptoken):
    maintenance_notifications_url = os.getenv("MAINTENANCE_NOTIFICATIONS_URL",
                               "https://id.api.s4hana.ondemand.com/sap/opu/odata/"
                               "sap/API_MAINTNOTIFICATION/MaintenanceNotification?$skiptoken={}".format(skiptoken))

    logger.debug("URL: {}, Username: {} , Password: {}".format(maintenance_notifications_url, user, password))

    # Making a get request
    response = requests.get(maintenance_notifications_url, auth=HTTPBasicAuth(user, password))
    response.raw.decode_content = True

    if response.status_code != 200:
        logger.error("Received status code: {}, Content {}".format(response.status_code, response.content))
        return None

    logger.info("Response status: {}".format(response.status_code))
    logger.info("Response content: {}".format(response.content))

    return response.content


def prepare_maintenance_notifications_long_text_data(mo_response):
    try:
        json_data = xmltodict.parse(mo_response)
        logger.info("Prepared JSON Content: {}".format(json_data))
        logger.info("Total entries: {}".format(len(json_data['feed']['entry'])))

        for e in json_data["feed"]["entry"]:
            try:
                keys = e['content']['m:properties'].keys()
                record = e['content']['m:properties']
                logger.debug("Keys: {}, record: {}".format(keys, record))

                MaintenanceNotification = str(record["d:MaintenanceNotification"])
                MaintNotifInternalID = str(record["d:MaintNotifInternalID"])
                NotificationText = str(record["d:NotificationText"])
                MaintPriority = str(record["d:MaintPriority"])
                NotificationType = str(record["d:NotificationType"])
                NotifProcessingPhase = str(record["d:NotifProcessingPhase"])
                NotifProcessingPhaseDesc = str(record["d:NotifProcessingPhaseDesc"])
                MaintPriorityDesc = str(record["d:MaintPriorityDesc"])
                if "null" in str(record["d:CreationDate"]):
                    CreationDate = np.nan
                else:
                    CreationDate = str(record["d:CreationDate"])
                if "null" in str(record["d:LastChangeTime"]):
                    LastChangeTime = np.nan
                else:
                    LastChangeTime = str(record["d:LastChangeTime"])
                if "null" in str(record["d:LastChangeDate"]):
                    LastChangeDate = np.nan
                else:
                    LastChangeDate = str(record["d:LastChangeDate"])
                if "null" in str(record["d:LastChangeDateTime"]):
                    LastChangeDateTime = np.nan
                else:
                    LastChangeDateTime = str(record["d:LastChangeDateTime"])
                CreationTime = str(record["d:CreationTime"])
                ReportedByUser = str(record["d:ReportedByUser"])
                ReporterFullName = str(record["d:ReporterFullName"])
                PersonResponsible = str(record["d:PersonResponsible"])
                MalfunctionEffect = str(record["d:MalfunctionEffect"])
                MalfunctionEffectText = str(record["d:MalfunctionEffectText"])
                if "null" in str(record["d:MalfunctionStartDate"]):
                    MalfunctionStartDate = np.nan
                else:
                    MalfunctionStartDate = str(record["d:MalfunctionStartDate"])
                if "null" in str(record["d:MalfunctionStartTime"]):
                    MalfunctionStartTime = np.nan
                else:
                    MalfunctionStartTime = str(record["d:MalfunctionStartTime"])
                if "null" in str(record["d:MalfunctionEndDate"]):
                    MalfunctionEndDate = np.nan
                else:
                    MalfunctionEndDate = str(record["d:MalfunctionEndDate"])
                if "null" in str(record["d:MalfunctionEndTime"]):
                    MalfunctionEndTime = np.nan
                else:
                    MalfunctionEndTime = str(record["d:MalfunctionEndTime"])
                MaintNotificationCatalog = str(record["d:MaintNotificationCatalog"])
                MaintNotificationCode = str(record["d:MaintNotificationCode"])
                MaintNotificationCodeGroup = str(record["d:MaintNotificationCodeGroup"])
                CatalogProfile = str(record["d:CatalogProfile"])
                if "null" in str(record["d:NotificationCreationDate"]):
                    NotificationCreationDate = np.nan
                else:
                    NotificationCreationDate = str(record["d:NotificationCreationDate"])
                if "null" in str(record["d:NotificationCreationTime"]):
                    NotificationCreationTime = np.nan
                else:
                    NotificationCreationTime = str(record["d:NotificationCreationTime"])
                NotificationTimeZone = str(record["d:NotificationTimeZone"])
                if "null" in str(record["d:RequiredStartDate"]):
                    RequiredStartDate = np.nan
                else:
                    RequiredStartDate = str(record["d:RequiredStartDate"])
                if "null" in str(record["d:RequiredStartTime"]):
                    RequiredStartTime = np.nan
                else:
                    RequiredStartTime = str(record["d:RequiredStartTime"])
                if "null" in str(record["d:RequiredEndDate"]):
                    RequiredEndDate = np.nan
                else:
                    RequiredEndDate = str(record["d:RequiredEndDate"])
                if "null" in str(record["d:RequiredEndTime"]):
                    RequiredEndTime = np.nan
                else:
                    RequiredEndTime = str(record["d:RequiredEndTime"])

                if "null" in str(record["d:LatestAcceptableCompletionDate"]):
                    LatestAcceptableCompletionDate = np.nan
                else:
                    LatestAcceptableCompletionDate = str(record["d:LatestAcceptableCompletionDate"])
                MaintenanceObjectIsDown = str(record["d:MaintenanceObjectIsDown"])
                MaintNotificationLongText = str(record["d:MaintNotificationLongText"])
                MaintNotifLongTextForEdit = str(record["d:MaintNotifLongTextForEdit"])
                TechnicalObject = str(record["d:TechnicalObject"])
                TechObjIsEquipOrFuncnlLoc = str(record["d:TechObjIsEquipOrFuncnlLoc"])
                TechnicalObjectLabel = str(record["d:TechnicalObjectLabel"])
                MaintenancePlanningPlant = str(record["d:MaintenancePlanningPlant"])
                MaintenancePlannerGroup = str(record["d:MaintenancePlannerGroup"])
                PlantSection = str(record["d:PlantSection"])
                ABCIndicator = str(record["d:ABCIndicator"])
                SuperiorTechnicalObject = str(record["d:SuperiorTechnicalObject"])
                SuperiorTechnicalObjectName = str(record["d:SuperiorTechnicalObjectName"])
                SuperiorObjIsEquipOrFuncnlLoc = str(record["d:SuperiorObjIsEquipOrFuncnlLoc"])
                SuperiorTechnicalObjectLabel = str(record["d:SuperiorTechnicalObjectLabel"])
                ManufacturerPartTypeName = str(record["d:ManufacturerPartTypeName"])
                TechObjIsEquipOrFuncnlLocDesc = str(record["d:TechObjIsEquipOrFuncnlLocDesc"])
                FunctionalLocation = str(record["d:FunctionalLocation"])
                TechnicalObjectDescription = str(record["d:TechnicalObjectDescription"])
                AssetLocation = str(record["d:AssetLocation"])
                LocationName = str(record["d:LocationName"])
                BusinessArea = str(record["d:BusinessArea"])
                CompanyCode = str(record["d:CompanyCode"])
                TechnicalObjectCategory = str(record["d:TechnicalObjectCategory"])
                TechnicalObjectType = str(record["d:TechnicalObjectType"])
                MainWorkCenterPlant = str(record["d:MainWorkCenterPlant"])
                MainWorkCenter = str(record["d:MainWorkCenter"])
                PlantName = str(record["d:PlantName"])
                MaintenancePlannerGroupName = str(record["d:MaintenancePlannerGroupName"])
                MaintenancePlant = str(record["d:MaintenancePlant"])
                LocationDescription = str(record["d:LocationDescription"])
                MainWorkCenterText = str(record["d:MainWorkCenterText"])
                MainWorkCenterPlantName = str(record["d:MainWorkCenterPlantName"])
                MaintenancePlantName = str(record["d:MaintenancePlantName"])
                PlantSectionPersonRespName = str(record["d:PlantSectionPersonRespName"])
                ABCIndicatorDesc = str(record["d:ABCIndicatorDesc"])
                PersonResponsibleName = str(record["d:PersonResponsibleName"])
                MaintenanceOrder = str(record["d:MaintenanceOrder"])
                MaintenanceOrderType = str(record["d:MaintenanceOrderType"])
                ConcatenatedActiveSystStsName = str(record["d:ConcatenatedActiveSystStsName"])
                MaintenanceActivityType = str(record["d:MaintenanceActivityType"])
                MaintObjDowntimeDurationUnit = str(record["d:MaintObjDowntimeDurationUnit"])
                MaintObjectDowntimeDuration = str(record["d:MaintObjectDowntimeDuration"])
                MaintenancePlan = str(record["d:MaintenancePlan"])
                MaintenanceItem = str(record["d:MaintenanceItem"])
                TaskListGroup = str(record["d:TaskListGroup"])
                TaskListGroupCounter = str(record["d:TaskListGroupCounter"])
                MaintenancePlanCallNumber = str(record["d:MaintenancePlanCallNumber"])
                MaintenanceTaskListType = str(record["d:MaintenanceTaskListType"])
                if "null" in str(record["d:NotificationReferenceDate"]):
                    NotificationReferenceDate = np.nan
                else:
                    NotificationReferenceDate = str(record["d:NotificationReferenceDate"])
                if "null" in str(record["d:NotificationReferenceTime"]):
                    NotificationReferenceTime = np.nan
                else:
                    NotificationReferenceTime = str(record["d:NotificationReferenceTime"])
                if "null" in str(record["d:NotificationCompletionDate"]):
                    NotificationCompletionDate = np.nan
                else:
                    NotificationCompletionDate = str(record["d:NotificationCompletionDate"])
                if "null" in str(record["d:CompletionTime"]):
                    CompletionTime = np.nan
                else:
                    CompletionTime = str(record["d:CompletionTime"])
                AssetRoom = str(record["d:AssetRoom"])
                MaintNotifExtReferenceNumber = str(record["d:MaintNotifExtReferenceNumber"])
                MaintNotifRejectionReasonCode = str(record["d:MaintNotifRejectionReasonCode"])
                MaintNotifRejectionRsnCodeTxt = str(record["d:MaintNotifRejectionRsnCodeTxt"])
                MaintNotifDetectionCodeText = str(record["d:MaintNotifDetectionCodeText"])
                MaintNotifDetectionCodeGrpTxt = str(record["d:MaintNotifDetectionCodeGrpTxt"])
                MaintNotifProcessPhaseCode = str(record["d:MaintNotifProcessPhaseCode"])
                MaintNotifProcessSubPhaseCode = str(record["d:MaintNotifProcessSubPhaseCode"])
                EAMProcessPhaseCodeDesc = str(record["d:EAMProcessPhaseCodeDesc"])
                EAMProcessSubPhaseCodeDesc = str(record["d:EAMProcessSubPhaseCodeDesc"])


                record = (MaintenanceNotification, MaintNotifInternalID, NotificationText, MaintPriority,
                          NotificationType, NotifProcessingPhase, NotifProcessingPhaseDesc, MaintPriorityDesc,
                          CreationDate, LastChangeTime, LastChangeDate, LastChangeDateTime, CreationTime,
                          ReportedByUser, ReporterFullName, PersonResponsible, MalfunctionEffect,
                          MalfunctionEffectText, MalfunctionStartDate, MalfunctionStartTime, MalfunctionEndDate,
                          MalfunctionEndTime, MaintNotificationCatalog, MaintNotificationCode,
                          MaintNotificationCodeGroup, CatalogProfile, NotificationCreationDate,
                          NotificationCreationTime, NotificationTimeZone, RequiredStartDate, RequiredStartTime,
                          RequiredEndDate, RequiredEndTime, LatestAcceptableCompletionDate, MaintenanceObjectIsDown,
                          MaintNotificationLongText, MaintNotifLongTextForEdit, TechnicalObject,
                          TechObjIsEquipOrFuncnlLoc, TechnicalObjectLabel, MaintenancePlanningPlant,
                          MaintenancePlannerGroup, PlantSection, ABCIndicator, SuperiorTechnicalObject,
                          SuperiorTechnicalObjectName, SuperiorObjIsEquipOrFuncnlLoc, SuperiorTechnicalObjectLabel,
                          ManufacturerPartTypeName, TechObjIsEquipOrFuncnlLocDesc, FunctionalLocation,
                          TechnicalObjectDescription, AssetLocation, LocationName, BusinessArea, CompanyCode,
                          TechnicalObjectCategory, TechnicalObjectType, MainWorkCenterPlant, MainWorkCenter, PlantName,
                          MaintenancePlannerGroupName, MaintenancePlant, LocationDescription, MainWorkCenterText,
                          MainWorkCenterPlantName, MaintenancePlantName, PlantSectionPersonRespName, ABCIndicatorDesc,
                          PersonResponsibleName, MaintenanceOrder, MaintenanceOrderType, ConcatenatedActiveSystStsName,
                          MaintenanceActivityType, MaintObjDowntimeDurationUnit, MaintObjectDowntimeDuration,
                          MaintenancePlan, MaintenanceItem, TaskListGroup, TaskListGroupCounter,
                          MaintenancePlanCallNumber, MaintenanceTaskListType, NotificationReferenceDate,
                          NotificationReferenceTime, NotificationCompletionDate, CompletionTime, AssetRoom,
                          MaintNotifExtReferenceNumber, MaintNotifRejectionReasonCode, MaintNotifRejectionRsnCodeTxt,
                          MaintNotifDetectionCodeText, MaintNotifDetectionCodeGrpTxt, MaintNotifProcessPhaseCode,
                          MaintNotifProcessSubPhaseCode, EAMProcessPhaseCodeDesc, EAMProcessSubPhaseCodeDesc)

                records.append(record)
            except Exception as e:
                logger.error("Error parsing data: {}".format(e))
                pass

    except Exception as e:
        logger.error("Exception raised while processing maintenance notification records: {}".format(e))
        return None


def prepare_mnlt_data_and_pd():
    next = True
    skiptoken = 0
    while next:
        result = get_maintenance_notifications_long_text_data(skiptoken)
        prepare_maintenance_notifications_long_text_data(result)
        json = xmltodict.parse(result)
        if "link" in json['feed'].keys():
            if len(json['feed']['link']) == 2:
                skiptoken = skiptoken + 100
            else:
                next = False

    logger.info("Total number of records: {}, columns: {}".format(len(records), len(columns)))
    df = create_pd(records, columns)
    logger.info("Current time: {}".format(datetime.datetime.now()))
    df['lastupdatedtime'] = str(datetime.datetime.now())

    return df


def prepare_mnlt_data_and_send_to_sql():
    table_name = "macmahon_maintenance_notifications_long_text"
    data = prepare_mnlt_data_and_pd()
    logger.info("Sending MO data to SQL: {}".format(data))
    if isinstance(data, pd.DataFrame):
        send_df_to_sql(data, table_name)

    return data


if __name__ == '__main__':
    prepare_mnlt_data_and_send_to_sql()
