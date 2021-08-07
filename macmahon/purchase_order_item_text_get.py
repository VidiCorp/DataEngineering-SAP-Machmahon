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
logger = logging.getLogger('PurchaseOrderItemText')

records = []
columns = ['PurchaseOrder', 'PurchaseOrderItem', 'PurchasingDocumentDeletionCode', 'PurchaseOrderItemText',
           'Plant', 'StorageLocation', 'MaterialGroup', 'PurchasingInfoRecord', 'SupplierMaterialNumber',
           'OrderQuantity', 'PurchaseOrderQuantityUnit', 'OrderPriceUnit', 'OrderPriceUnitToOrderUnitNmrtr',
           'OrdPriceUnitToOrderUnitDnmntr', 'DocumentCurrency', 'NetPriceAmount', 'NetPriceQuantity', 'TaxCode',
           'TaxDeterminationDate', 'TaxCountry', 'PriceIsToBePrinted', 'OverdelivTolrtdLmtRatioInPct',
           'UnlimitedOverdeliveryIsAllowed', 'UnderdelivTolrtdLmtRatioInPct', 'ValuationType', 'IsCompletelyDelivered',
           'IsFinallyInvoiced', 'PurchaseOrderItemCategory', 'AccountAssignmentCategory',
           'MultipleAcctAssgmtDistribution', 'PartialInvoiceDistribution', 'GoodsReceiptIsExpected',
           'GoodsReceiptIsNonValuated', 'InvoiceIsExpected', 'InvoiceIsGoodsReceiptBased', 'PurchaseContract',
           'PurchaseContractItem', 'Customer', 'Subcontractor', 'SupplierIsSubcontractor', 'ItemNetWeight',
           'ItemWeightUnit', 'TaxJurisdiction', 'PricingDateControl', 'ItemVolume', 'ItemVolumeUnit',
           'SupplierConfirmationControlKey', 'IncotermsClassification', 'IncotermsTransferLocation',
           'EvaldRcptSettlmtIsAllowed', 'PurchaseRequisition', 'PurchaseRequisitionItem', 'IsReturnsItem',
           'ServicePackage', 'EarmarkedFunds', 'EarmarkedFundsDocument', 'EarmarkedFundsItem',
           'EarmarkedFundsDocumentItem', 'IncotermsLocation1', 'IncotermsLocation2', 'Material',
           'InternationalArticleNumber', 'ManufacturerMaterial', 'ServicePerformer', 'ProductType',
           'ExpectedOverallLimitAmount', 'OverallLimitAmount', 'PurContractForOverallLimit',
           'ReferenceDeliveryAddressID', 'DeliveryAddressID', 'DeliveryAddressName', 'DeliveryAddressName2',
           'DeliveryAddressFullName', 'DeliveryAddressStreetName', 'DeliveryAddressHouseNumber',
           'DeliveryAddressCityName', 'DeliveryAddressPostalCode', 'DeliveryAddressRegion', 'DeliveryAddressCountry',
           'DownPaymentType', 'DownPaymentPercentageOfTotAmt', 'DownPaymentAmount', 'DownPaymentDueDate',
           'BR_MaterialUsage', 'BR_MaterialOrigin', 'BR_CFOPCategory', 'BR_IsProducedInHouse', 'ConsumptionTaxCtrlCode',
           'PurgProdCmplncSupplierStatus', 'PurgProductMarketabilityStatus', 'PurgSafetyDataSheetStatus',
           'PurgProdCmplncDngrsGoodsStatus']


def get_purchase_order_item_text_data(skiptoken):
    maintenance_notifications_url = os.getenv("PURCHASE_ORDER_TEXT_URL",
                               "https://id.api.s4hana.ondemand.com/sap/opu/odata/sap/"
                               "API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrderItem?$skiptoken={}".format(skiptoken))

    logger.debug("URL: {}, Username: {} , Password: {}".format(maintenance_notifications_url, user, password))

    # Making a get request
    response = requests.get(maintenance_notifications_url, auth=HTTPBasicAuth(user, password))
    response.raw.decode_content = True

    if response.status_code != 200:
        logger.error("Received status code: {}, Content {}".format(response.status_code, response.content))
        return None

    print("Response status: {}".format(response.status_code))
    print("Response content: {}".format(response.content))

    return response.content


def prepare_purchase_order_item_text_data(mo_response):
    try:
        json_data = xmltodict.parse(mo_response)
        print("Prepared JSON Content: {}".format(json_data))
        print("Total entries: {}".format(len(json_data['feed']['entry'])))

        for e in json_data["feed"]["entry"]:
            try:
                keys = e['content']['m:properties'].keys()
                record = e['content']['m:properties']
                logger.debug("Keys: {}, record: {}".format(keys, record))

                PurchaseOrder = str(record["d:PurchaseOrder"])
                PurchaseOrderItem = str(record["d:PurchaseOrderItem"])
                PurchasingDocumentDeletionCode = str(record["d:PurchasingDocumentDeletionCode"])
                PurchaseOrderItemText = str(record["d:PurchaseOrderItemText"])
                Plant = str(record["d:Plant"])
                StorageLocation = str(record["d:StorageLocation"])
                MaterialGroup = str(record["d:MaterialGroup"])
                PurchasingInfoRecord = str(record["d:PurchasingInfoRecord"])
                SupplierMaterialNumber = str(record["d:SupplierMaterialNumber"])
                OrderQuantity = str(record["d:OrderQuantity"])
                PurchaseOrderQuantityUnit = str(record["d:PurchaseOrderQuantityUnit"])
                OrderPriceUnit = str(record["d:OrderPriceUnit"])
                OrderPriceUnitToOrderUnitNmrtr = str(record["d:OrderPriceUnitToOrderUnitNmrtr"])
                OrdPriceUnitToOrderUnitDnmntr = str(record["d:OrdPriceUnitToOrderUnitDnmntr"])
                DocumentCurrency = str(record["d:DocumentCurrency"])
                NetPriceAmount = str(record["d:NetPriceAmount"])
                NetPriceQuantity = str(record["d:NetPriceQuantity"])
                TaxCode = str(record["d:TaxCode"])
                if "null" in str(record["d:TaxDeterminationDate"]):
                    TaxDeterminationDate = np.nan
                else:
                    TaxDeterminationDate = str(record["d:TaxDeterminationDate"])
                TaxCountry = str(record["d:TaxCountry"])
                PriceIsToBePrinted = str(record["d:PriceIsToBePrinted"])
                OverdelivTolrtdLmtRatioInPct = str(record["d:OverdelivTolrtdLmtRatioInPct"])
                UnlimitedOverdeliveryIsAllowed = str(record["d:UnlimitedOverdeliveryIsAllowed"])
                UnderdelivTolrtdLmtRatioInPct = str(record["d:UnderdelivTolrtdLmtRatioInPct"])
                ValuationType = str(record["d:ValuationType"])
                IsCompletelyDelivered = str(record["d:IsCompletelyDelivered"])
                IsFinallyInvoiced = str(record["d:IsFinallyInvoiced"])
                PurchaseOrderItemCategory = str(record["d:PurchaseOrderItemCategory"])
                AccountAssignmentCategory = str(record["d:AccountAssignmentCategory"])
                MultipleAcctAssgmtDistribution = str(record["d:MultipleAcctAssgmtDistribution"])
                PartialInvoiceDistribution = str(record["d:PartialInvoiceDistribution"])
                GoodsReceiptIsExpected = str(record["d:GoodsReceiptIsExpected"])
                GoodsReceiptIsNonValuated = str(record["d:GoodsReceiptIsNonValuated"])
                InvoiceIsExpected = str(record["d:InvoiceIsExpected"])
                InvoiceIsGoodsReceiptBased = str(record["d:InvoiceIsGoodsReceiptBased"])
                PurchaseContract = str(record["d:PurchaseContract"])
                PurchaseContractItem = str(record["d:PurchaseContractItem"])
                Customer = str(record["d:Customer"])
                Subcontractor = str(record["d:Subcontractor"])
                SupplierIsSubcontractor = str(record["d:SupplierIsSubcontractor"])
                ItemNetWeight = str(record["d:ItemNetWeight"])
                ItemWeightUnit = str(record["d:ItemWeightUnit"])
                TaxJurisdiction = str(record["d:TaxJurisdiction"])
                PricingDateControl = str(record["d:PricingDateControl"])
                ItemVolume = float(record["d:ItemVolume"])
                ItemVolumeUnit = str(record["d:ItemVolumeUnit"])
                SupplierConfirmationControlKey = str(record["d:SupplierConfirmationControlKey"])
                IncotermsClassification = str(record["d:IncotermsClassification"])
                IncotermsTransferLocation = str(record["d:IncotermsTransferLocation"])
                EvaldRcptSettlmtIsAllowed = str(record["d:EvaldRcptSettlmtIsAllowed"])
                PurchaseRequisition = str(record["d:PurchaseRequisition"])
                PurchaseRequisitionItem = str(record["d:PurchaseRequisitionItem"])
                IsReturnsItem = str(record["d:IsReturnsItem"])
                ServicePackage = str(record["d:ServicePackage"])
                EarmarkedFunds = str(record["d:EarmarkedFunds"])
                EarmarkedFundsDocument = str(record["d:EarmarkedFundsDocument"])
                EarmarkedFundsItem = str(record["d:EarmarkedFundsItem"])
                EarmarkedFundsDocumentItem = str(record["d:EarmarkedFundsDocumentItem"])
                IncotermsLocation1 = str(record["d:IncotermsLocation1"])
                IncotermsLocation2 = str(record["d:IncotermsLocation2"])
                Material = str(record["d:Material"])
                InternationalArticleNumber = str(record["d:InternationalArticleNumber"])
                ManufacturerMaterial = str(record["d:ManufacturerMaterial"])
                ServicePerformer = str(record["d:ServicePerformer"])
                ProductType = str(record["d:ProductType"])
                ExpectedOverallLimitAmount = float(record["d:ExpectedOverallLimitAmount"])
                OverallLimitAmount = float(record["d:OverallLimitAmount"])
                PurContractForOverallLimit = str(record["d:PurContractForOverallLimit"])
                ReferenceDeliveryAddressID = str(record["d:ReferenceDeliveryAddressID"])
                DeliveryAddressID = str(record["d:DeliveryAddressID"])
                DeliveryAddressName = str(record["d:DeliveryAddressName"])
                DeliveryAddressName2 = str(record["d:DeliveryAddressName2"])
                DeliveryAddressFullName = str(record["d:DeliveryAddressFullName"])
                DeliveryAddressStreetName = str(record["d:DeliveryAddressStreetName"])
                DeliveryAddressHouseNumber = str(record["d:DeliveryAddressHouseNumber"])
                DeliveryAddressCityName = str(record["d:DeliveryAddressCityName"])
                DeliveryAddressPostalCode = str(record["d:DeliveryAddressPostalCode"])
                DeliveryAddressRegion = str(record["d:DeliveryAddressRegion"])
                DeliveryAddressCountry = str(record["d:DeliveryAddressCountry"])
                DownPaymentType = str(record["d:DownPaymentType"])
                DownPaymentPercentageOfTotAmt = float(record["d:DownPaymentPercentageOfTotAmt"])
                DownPaymentAmount = float(record["d:DownPaymentAmount"])
                if "null" in str(record["d:DownPaymentDueDate"]):
                    DownPaymentDueDate = np.nan
                else:
                    DownPaymentDueDate = str(record["d:DownPaymentDueDate"])
                BR_MaterialUsage = str(record["d:BR_MaterialUsage"])
                BR_MaterialOrigin = str(record["d:BR_MaterialOrigin"])
                BR_CFOPCategory = str(record["d:BR_CFOPCategory"])
                BR_IsProducedInHouse = str(record["d:BR_IsProducedInHouse"])
                ConsumptionTaxCtrlCode = str(record["d:ConsumptionTaxCtrlCode"])
                PurgProdCmplncSupplierStatus = str(record["d:PurgProdCmplncSupplierStatus"])
                PurgProductMarketabilityStatus = str(record["d:PurgProductMarketabilityStatus"])
                PurgSafetyDataSheetStatus = str(record["d:PurgSafetyDataSheetStatus"])
                PurgProdCmplncDngrsGoodsStatus = str(record["d:PurgProdCmplncDngrsGoodsStatus"])

                record = (PurchaseOrder, PurchaseOrderItem, PurchasingDocumentDeletionCode, PurchaseOrderItemText,
                          Plant, StorageLocation, MaterialGroup, PurchasingInfoRecord, SupplierMaterialNumber,
                          OrderQuantity, PurchaseOrderQuantityUnit, OrderPriceUnit, OrderPriceUnitToOrderUnitNmrtr,
                          OrdPriceUnitToOrderUnitDnmntr, DocumentCurrency, NetPriceAmount, NetPriceQuantity, TaxCode,
                          TaxDeterminationDate, TaxCountry, PriceIsToBePrinted, OverdelivTolrtdLmtRatioInPct,
                          UnlimitedOverdeliveryIsAllowed, UnderdelivTolrtdLmtRatioInPct, ValuationType,
                          IsCompletelyDelivered, IsFinallyInvoiced, PurchaseOrderItemCategory,
                          AccountAssignmentCategory, MultipleAcctAssgmtDistribution, PartialInvoiceDistribution,
                          GoodsReceiptIsExpected, GoodsReceiptIsNonValuated, InvoiceIsExpected,
                          InvoiceIsGoodsReceiptBased, PurchaseContract, PurchaseContractItem, Customer, Subcontractor,
                          SupplierIsSubcontractor, ItemNetWeight, ItemWeightUnit, TaxJurisdiction, PricingDateControl,
                          ItemVolume, ItemVolumeUnit, SupplierConfirmationControlKey, IncotermsClassification,
                          IncotermsTransferLocation, EvaldRcptSettlmtIsAllowed, PurchaseRequisition,
                          PurchaseRequisitionItem, IsReturnsItem, ServicePackage, EarmarkedFunds,
                          EarmarkedFundsDocument, EarmarkedFundsItem, EarmarkedFundsDocumentItem, IncotermsLocation1,
                          IncotermsLocation2, Material, InternationalArticleNumber, ManufacturerMaterial,
                          ServicePerformer, ProductType, ExpectedOverallLimitAmount, OverallLimitAmount,
                          PurContractForOverallLimit, ReferenceDeliveryAddressID, DeliveryAddressID,
                          DeliveryAddressName, DeliveryAddressName2, DeliveryAddressFullName, DeliveryAddressStreetName,
                          DeliveryAddressHouseNumber, DeliveryAddressCityName, DeliveryAddressPostalCode,
                          DeliveryAddressRegion, DeliveryAddressCountry, DownPaymentType, DownPaymentPercentageOfTotAmt,
                          DownPaymentAmount, DownPaymentDueDate, BR_MaterialUsage, BR_MaterialOrigin, BR_CFOPCategory,
                          BR_IsProducedInHouse, ConsumptionTaxCtrlCode, PurgProdCmplncSupplierStatus,
                          PurgProductMarketabilityStatus, PurgSafetyDataSheetStatus, PurgProdCmplncDngrsGoodsStatus)

                records.append(record)
            except Exception as e:
                logger.error("Error parsing data: {}".format(e))
                pass

    except Exception as e:
        logger.error("Exception raised while processing maintenance notification records: {}".format(e))
        return None


def prepare_poit_data_and_pd():
    next = True
    skiptoken = 0
    while next:
        result = get_purchase_order_item_text_data(skiptoken)
        prepare_purchase_order_item_text_data(result)
        json = xmltodict.parse(result)
        if "link" in json['feed'].keys():
            if len(json['feed']['link']) == 2:
                skiptoken = skiptoken + 1000
            else:
                next = False

    print("Total number of records: {}, columns: {}".format(len(records), len(columns)))
    df = create_pd(records, columns)
    print("Current time: {}".format(datetime.datetime.now()))
    df['lastupdatedtime'] = str(datetime.datetime.now())

    return df


def prepare_poit_data_and_send_to_sql():
    table_name = "macmahon_purchase_order_item_text"
    data = prepare_poit_data_and_pd()
    print("Sending MO data to SQL: {}".format(data))
    if isinstance(data, pd.DataFrame):
        send_df_to_sql(data, table_name)

    return data


if __name__ == '__main__':
    prepare_poit_data_and_send_to_sql()
