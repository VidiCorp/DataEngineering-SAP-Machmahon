import os
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

logging.basicConfig(level="INFO")
logger = logging.getLogger('PRODUCT_DESCRIPTION')

user = os.getenv("SAP_USER", "SAP_USERNAME")
password = os.getenv("PASSWORD", "SAP_PASSWORD")

records = []
product_description_columns = ["Product", "Language", "ProductDescription"]


def get_product_description(skiptoken):
    logger.info("Fetching records from: {}".format(skiptoken))
    document_header_url = os.getenv("PRODUCT_DESCRIPTION_URL", "https://id.api.s4hana.ondemand.com/sap/opu/"
                                                           "odata/sap/API_PRODUCT_SRV/A_ProductDescription?"
                                                           "$skiptoken={}").format(skiptoken)

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


def prepare_product_description_data(pd_response):

    try:
        json_data = xmltodict.parse(pd_response)
        logger.info("Prepared JSON Content: {}".format(json_data))
        logger.info("Total entries: {}".format(len(json_data['feed']['entry'])))

        for e in json_data["feed"]["entry"]:
            try:
                record = e['content']['m:properties']
                keys = e['content']['m:properties'].keys()
                logger.debug("Keys: {}, record: {}".format(keys, record))

                Product = str(record["d:Product"])
                Language = str(record["d:Language"])
                ProductDescription = str(record["d:ProductDescription"])

                record_tuple = (Product, Language, ProductDescription)

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
        result = get_product_description(skiptoken)
        prepare_product_description_data(result)
        json = xmltodict.parse(result)
        if "link" in json['feed'].keys():
            if len(json['feed']['link']) == 2:
                skiptoken = skiptoken + 1000
            else:
                next = False

    logger.info("Total number of columns: {}".format(len(product_description_columns)))
    df = create_pd(records, product_description_columns)
    logger.info("Current time: {}".format(datetime.datetime.now()))
    df['lastupdatedtime'] = str(datetime.datetime.now())

    return df


def prepare_dh_data_and_send_to_sql():
    table_name = "macmahon_product_description"
    data = prepare_dh_data_and_pd()
    logger.debug("Sending PO data to SQL: {}".format(data))
    if isinstance(data, pd.DataFrame):
        send_df_to_sql(data, table_name)

    return data


if __name__ == '__main__':
    prepare_dh_data_and_send_to_sql()
