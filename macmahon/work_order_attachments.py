import os
import sys
import logging
import requests
from requests.auth import HTTPBasicAuth
import xmltodict

import pandas as pd

import datetime
from datetime import timedelta

from utils import send_df_to_sql
from utils import create_pd

logging.basicConfig(level="INFO")
logger = logging.getLogger('WORK_ORDER_ATTACHMENTS')

DEFAULT_INT_VALUE = 0
DEFAULT_STRING_VALUE = ''

user = os.getenv("SAP_USER", "SAP_USERNAME")
password = os.getenv("PASSWORD", "SAP_PASSWORD")

records = []
attachment_records = []
wo_columns = ["ID"]


def download_attachment(DocumentInfoRecordDocType, DocumentInfoRecordDocNumber, DocumentInfoRecordDocPart,
                        DocumentInfoRecordDocVersion, LogicalDocument, ArchiveDocumentID, LinkedSAPObjectKey,
                        BusinessObjectTypeName):
    url = "https://id.api.s4hana.ondemand.com/sap/opu/odata/sap/" \
          "API_CV_ATTACHMENT_SRV/AttachmentContentSet(" \
          "DocumentInfoRecordDocType='{}'" \
          ",DocumentInfoRecordDocNumber='{}'," \
          "DocumentInfoRecordDocPart='{}'," \
          "DocumentInfoRecordDocVersion='{}'," \
          "LogicalDocument='{}'," \
          "ArchiveDocumentID='{}'," \
          "LinkedSAPObjectKey='{}'," \
          "BusinessObjectTypeName='{}')/$value".format(DocumentInfoRecordDocType,
                                                       DocumentInfoRecordDocNumber,
                                                       DocumentInfoRecordDocPart,
                                                       DocumentInfoRecordDocVersion,
                                                       LogicalDocument,
                                                       ArchiveDocumentID,
                                                       LinkedSAPObjectKey,
                                                       BusinessObjectTypeName
                                                       )
    r = requests.get(url, stream=True, auth=HTTPBasicAuth(user, password))
    logger.debug("Status Code: {}".format(r.status_code))
    if r.status_code == 200:
        return r.content

    return None


def get_attachment_data(worker_order_number):
    attachment_url = "https://my301469.s4hana.ondemand.com/sap/opu/odata/sap/" \
                     "API_CV_ATTACHMENT_SRV/GetAllOriginals?LinkedSAPObjectKey='{}'" \
                     "&BusinessObjectTypeName='PMAUFK'".format(worker_order_number)
    params = {
        "LinkedSAPObjectKey": '{}'.format(worker_order_number),
        "BusinessObjectTypeName": 'PMAUFK'
    }

    response = requests.get(attachment_url, auth=HTTPBasicAuth(user, password))

    if response.status_code == 200:
        logger.debug("Response content: {}".format(response.content))
        return response
    else:
        logger.error("Could not retrieve data for work order: {}, Status_code: {}, Message: {}".
                     format(worker_order_number, response.status_code, response.content))

    return response


def get_work_order_data(skiptoken):
    dt = datetime.datetime.now() - timedelta(days=14)
    new_format = "%Y-%m-%dT%H:%M:%SZ"
    new_dt = str(dt.strftime(new_format))

    logger.info("Processing data from: {}, date: {}".format(skiptoken, new_dt))
    work_order_url = os.getenv("WORK_ORDER_URL",
                               "https://id.api.s4hana.ondemand.com/sap/opu/odata/"
                               "sap/YY1_WORKORDER_CDS/YY1_WorkOrder/")

    params = {
        "$skiptoken": skiptoken,
        "$filter": "LastChangeDateTime ge datetimeoffset'{}'".format(new_dt)
    }

    logger.debug("URL: {}, Username: {} , Password: {}".format(work_order_url, user, password))

    # Making a get request
    response = requests.get(work_order_url, params=params, auth=HTTPBasicAuth(user, password))
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

            record_tuple = (ID)

            records.append(record_tuple)

    except Exception as e:
        logger.error("Exception raised while preparing data: {}".format(e))
        return None


def get_attachment_records(attachment_record):
    json_data = xmltodict.parse(attachment_record)

    try:
        if "entry" in json_data['feed'].keys():
            logger.info("Processing entries for type: {}".format(type(json_data['feed']['entry'])))
            if type(json_data['feed']['entry']) is list:
                for entry in json_data['feed']['entry']:
                    id = entry["id"]
                    DocumentInfoRecordDocType = entry["m:properties"]['d:DocumentInfoRecordDocType']
                    DocumentInfoRecordDocNumber = entry["m:properties"]['d:DocumentInfoRecordDocNumber']
                    DocumentInfoRecordDocPart = entry["m:properties"]['d:DocumentInfoRecordDocPart']
                    DocumentInfoRecordDocVersion = entry["m:properties"]['d:DocumentInfoRecordDocVersion']
                    LogicalDocument = entry["m:properties"]['d:LogicalDocument']
                    ArchiveDocumentID = entry["m:properties"]['d:ArchiveDocumentID']
                    LinkedSAPObjectKey = entry["m:properties"]['d:LinkedSAPObjectKey']
                    BusinessObjectTypeName = entry["m:properties"]['d:BusinessObjectTypeName']
                    attachment = download_attachment(DocumentInfoRecordDocType, DocumentInfoRecordDocNumber,
                                                     DocumentInfoRecordDocPart, DocumentInfoRecordDocVersion,
                                                     LogicalDocument, ArchiveDocumentID, LinkedSAPObjectKey,
                                                     BusinessObjectTypeName)

                    record = (id, DocumentInfoRecordDocType, DocumentInfoRecordDocNumber, DocumentInfoRecordDocPart,
                              DocumentInfoRecordDocVersion, LogicalDocument, ArchiveDocumentID, LinkedSAPObjectKey,
                              BusinessObjectTypeName, attachment)
                    attachment_records.append(record)

            else:
                id = json_data["feed"]["entry"]["id"]
                DocumentInfoRecordDocType = json_data["feed"]["entry"]["m:properties"]['d:DocumentInfoRecordDocType']
                DocumentInfoRecordDocNumber = json_data["feed"]["entry"]["m:properties"]['d:DocumentInfoRecordDocNumber']
                DocumentInfoRecordDocPart = json_data["feed"]["entry"]["m:properties"]['d:DocumentInfoRecordDocPart']
                DocumentInfoRecordDocVersion = json_data["feed"]["entry"]["m:properties"]['d:DocumentInfoRecordDocVersion']
                LogicalDocument = json_data["feed"]["entry"]["m:properties"]['d:LogicalDocument']
                ArchiveDocumentID = json_data["feed"]["entry"]["m:properties"]['d:ArchiveDocumentID']
                LinkedSAPObjectKey = json_data["feed"]["entry"]["m:properties"]['d:LinkedSAPObjectKey']
                BusinessObjectTypeName = json_data["feed"]["entry"]["m:properties"]['d:BusinessObjectTypeName']
                attachment = download_attachment(DocumentInfoRecordDocType, DocumentInfoRecordDocNumber,
                                                 DocumentInfoRecordDocPart, DocumentInfoRecordDocVersion,
                                                 LogicalDocument, ArchiveDocumentID, LinkedSAPObjectKey,
                                                 BusinessObjectTypeName)
                record = (id, DocumentInfoRecordDocType, DocumentInfoRecordDocNumber, DocumentInfoRecordDocPart,
                          DocumentInfoRecordDocVersion, LogicalDocument, ArchiveDocumentID,
                          LinkedSAPObjectKey, BusinessObjectTypeName, attachment)

                attachment_records.append(record)

    except Exception as e:
        logger.error(
            "Exception raised while processing record: {}, message: {}".format(attachment_record, e))
        return None


def prepare_attachments_data(work_orders):
    columns = ["id", "DocumentInfoRecordDocType", "DocumentInfoRecordDocNumber", "DocumentInfoRecordDocPart",
               "DocumentInfoRecordDocVersion", "LogicalDocument", "ArchiveDocumentID", "LinkedSAPObjectKey",
               "BusinessObjectTypeName", "attachment"]
    for wo in work_orders:
        resp = get_attachment_data(wo)
        if resp.status_code == 200:
            get_attachment_records(resp.content)
        else:
            logger.info("Could not retrieve data for wo: {}".format(wo))

    df = create_pd(attachment_records, columns)
    df['lastupdatedtime'] = str(datetime.datetime.now())
    return df


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


def prepare_attachment_data_and_send_to_sql(work_order_numbers):
    table_name = "macmahon_work_order_attachments"
    df = prepare_attachments_data(work_order_numbers)
    logger.info("Sending attachment data {} to SQL".format(df))
    send_df_to_sql(df, table_name)


if __name__ == '__main__':
    records_df = prepare_wo_data_and_pd()
    if records_df is None:
        logger.error("Error processing records, did not receive a dataframe output")
        sys.exit(1)
    wo_nums = records_df["ID"].tolist()
    logger.info("Total work orders to process: {}".format(len(wo_nums)))
    wo_nums = wo_nums[6600:]
    logger.info("WorkOrders: {}".format(wo_nums))
    prepare_attachment_data_and_send_to_sql(wo_nums)
