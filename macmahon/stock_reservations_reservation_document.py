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
logger = logging.getLogger('RESERVATION_DOCUMENT')

user = os.getenv("SAP_USER", "SAP_USERNAME")
password = os.getenv("PASSWORD", "SAP_PASSWORD")

records = []
reservation_document_columns = ["Reservation", "ReservationItem", "RecordType", "Product", "RequirementType",
                                "MatlCompRequirementDate", "Plant", "ManufacturingOrderOperation",
                                "GoodsMovementIsAllowed", "StorageLocation", "Batch", "DebitCreditCode",
                                "BaseUnit", "GLAccount", "ResvnAccountIsEnteredManually", "GoodsMovementType",
                                "EntryUnit", "CompanyCodeCurrency", "IssuingOrReceivingPlant",
                                "IssuingOrReceivingStorageLoc", "PurchasingDocument", "PurchasingDocumentItem",
                                "Supplier", "ResvnItmRequiredQtyInBaseUnit", "ReservationItemIsFinallyIssued",
                                "ReservationItmIsMarkedForDeltn", "ResvnItmRequiredQtyInBaseUnit",
                                "ResvnItmRequiredQtyInEntryUnit", "ResvnItmRequiredQtyInBaseUnit",
                                "ResvnItmWithdrawnQtyInBaseUnit", "ResvnItmWithdrawnAmtInCCCrcy", "GoodsRecipientName",
                                " UnloadingPointName", "ReservationItemText"]


def get_reservation_document(skiptoken):
    logger.info("Fetching records from: {}".format(skiptoken))
    document_header_url = os.getenv("DOCUMENT_HEADER_URL", "https://id.api.s4hana.ondemand.com/sap/opu/"
                                                          "odata/sap/API_RESERVATION_DOCUMENT_SRV/"
                                                          "A_ReservationDocumentItem")

    logger.debug("URL: {}, Username: {} , Password: {}".format(document_header_url, user, password))
    params = {
        "skiptoken" : skiptoken,
        "$select" : "Reservation,ReservationItem,RecordType,Product,RequirementType,"
                    "MatlCompRequirementDate,Plant,ManufacturingOrderOperation,GoodsMovementIsAllowed,"
                    "StorageLocation,Batch,DebitCreditCode,BaseUnit,GLAccount,ResvnAccountIsEnteredManually,"
                    "GoodsMovementType,EntryUnit,CompanyCodeCurrency,IssuingOrReceivingPlant,"
                    "IssuingOrReceivingStorageLoc,PurchasingDocument,PurchasingDocumentItem,"
                    "Supplier,ResvnItmRequiredQtyInBaseUnit,ReservationItemIsFinallyIssued,"
                    "ReservationItmIsMarkedForDeltn,ResvnItmRequiredQtyInBaseUnit,ResvnItmRequiredQtyInEntryUnit,"
                    "ResvnItmRequiredQtyInBaseUnit,ResvnItmWithdrawnQtyInBaseUnit,"
                    "ResvnItmWithdrawnAmtInCCCrcy,GoodsRecipientName,"
                    "UnloadingPointName,ReservationItemText"
    }
    # Making a get request
    response = requests.get(document_header_url, params=params, auth=HTTPBasicAuth(user, password))
    response.raw.decode_content = True

    if response.status_code != 200:
        logger.error("Received status code: {}, Content {}".format(response.status_code, response.content))
        return None

    logger.info("Response status: {}".format(response.status_code))
    logger.info("Response content: {}".format(response.content))

    return response.content


def prepare_reservation_document(rd_response):

    try:
        json_data = xmltodict.parse(rd_response)
        logger.info("Prepared JSON Content: {}".format(json_data))
        logger.info("Total entries: {}".format(len(json_data['feed']['entry'])))

        for e in json_data["feed"]["entry"]:
            try:
                record = e['content']['m:properties']
                keys = e['content']['m:properties'].keys()
                logger.debug("Keys: {}, record: {}".format(keys, record))

                Reservation = str(record["d:Reservation"])
                ReservationItem = str(record["d:ReservationItem"])
                RecordType = str(record["d:RecordType"])
                Product = str(record["d:Product"])
                RequirementType = str(record["d:RequirementType"])
                if "null" in str(record["d:MatlCompRequirementDate"]):
                    MatlCompRequirementDate = np.nan
                else:
                    MatlCompRequirementDate = str(record["d:MatlCompRequirementDate"])
                Plant = str(record["d:Plant"])
                ManufacturingOrderOperation = str(record["d:ManufacturingOrderOperation"])
                GoodsMovementIsAllowed = str(record["d:GoodsMovementIsAllowed"])
                StorageLocation = str(record["d:StorageLocation"])
                Batch = str(record["d:Batch"])
                DebitCreditCode = str(record["d:DebitCreditCode"])
                BaseUnit = str(record["d:BaseUnit"])
                GLAccount = str(record["d:GLAccount"])
                ResvnAccountIsEnteredManually = str(record["d:ResvnAccountIsEnteredManually"])
                GoodsMovementType = str(record["d:GoodsMovementType"])
                EntryUnit = str(record["d:EntryUnit"])
                # QuantityIsFixed = str(record["d:QuantityIsFixed"])
                CompanyCodeCurrency = str(record["d:CompanyCodeCurrency"])
                IssuingOrReceivingPlant = str(record["d:IssuingOrReceivingPlant"])
                IssuingOrReceivingStorageLoc = str(record["d:IssuingOrReceivingStorageLoc"])
                PurchasingDocument = str(record["d:PurchasingDocument"])
                PurchasingDocumentItem = str(record["d:PurchasingDocumentItem"])
                Supplier = str(record["d:Supplier"])
                ResvnItmRequiredQtyInBaseUnit = str(record["d:ResvnItmRequiredQtyInBaseUnit"])
                ReservationItemIsFinallyIssued = str(record["d:ReservationItemIsFinallyIssued"])
                ReservationItmIsMarkedForDeltn = str(record["d:ReservationItmIsMarkedForDeltn"])
                ResvnItmRequiredQtyInEntryUnit = str(record["d:ResvnItmRequiredQtyInEntryUnit"])
                ResvnItmWithdrawnQtyInBaseUnit = str(record["d:ResvnItmWithdrawnQtyInBaseUnit"])
                ResvnItmWithdrawnAmtInCCCrcy = str(record["d:ResvnItmWithdrawnAmtInCCCrcy"])
                GoodsRecipientName = str(record["d:GoodsRecipientName"])
                UnloadingPointName = str(record["d:UnloadingPointName"])
                ReservationItemText = str(record["d:ReservationItemText"])

                record_tuple = (Reservation, ReservationItem, RecordType, Product, RequirementType,
                                MatlCompRequirementDate, Plant, ManufacturingOrderOperation, GoodsMovementIsAllowed,
                                StorageLocation, Batch, DebitCreditCode, BaseUnit, GLAccount, ResvnAccountIsEnteredManually,
                                GoodsMovementType, EntryUnit, CompanyCodeCurrency, IssuingOrReceivingPlant,
                                IssuingOrReceivingStorageLoc, PurchasingDocument, PurchasingDocumentItem, Supplier,
                                ResvnItmRequiredQtyInBaseUnit, ReservationItemIsFinallyIssued, ReservationItmIsMarkedForDeltn,
                                ResvnItmRequiredQtyInBaseUnit, ResvnItmRequiredQtyInEntryUnit, ResvnItmRequiredQtyInBaseUnit,
                                ResvnItmWithdrawnQtyInBaseUnit, ResvnItmWithdrawnAmtInCCCrcy, GoodsRecipientName,
                                UnloadingPointName, ReservationItemText
                                )
                records.append(record_tuple)

            except Exception as e:
                logger.error("Exception raised while processing record: {}, message: {}".format(record, e))

    except Exception as e:
        logger.error("Error preparing document header records: {}".format(e))
        return None


def prepare_rd_data_and_pd():
    next = True
    skiptoken = 0
    while next:
        result = get_reservation_document(skiptoken)
        prepare_reservation_document(result)
        json = xmltodict.parse(result)
        if "link" in json['feed'].keys():
            if len(json['feed']['link']) == 2:
                skiptoken = skiptoken + 1000
            else:
                next = False

    logger.info("Total number of columns: {}".format(len(reservation_document_columns)))
    df = create_pd(records, reservation_document_columns)
    logger.info("Current time: {}".format(datetime.datetime.now()))
    df['lastupdatedtime'] = str(datetime.datetime.now())

    return df


def prepare_rd_data_and_send_to_sql():
    table_name = "macmahon_reservation_document"
    data = prepare_rd_data_and_pd()
    logger.debug("Sending PO data to SQL: {}".format(data))
    if isinstance(data, pd.DataFrame):
        send_df_to_sql(data, table_name)

    return data


if __name__ == '__main__':
    prepare_rd_data_and_send_to_sql()
