import sys
import os

import logging as logger
import pandas as pd

from urllib.parse import quote_plus
from sqlalchemy import create_engine, event


def send_df_to_sql(data, table_name, mode="replace"):

    server_name = os.getenv("SERVER_NAME", "mah-azu-sql01.database.windows.net")
    db_nam = os.getenv("DB_NAME", "DevMacDB")
    username = os.getenv("DB_USER", "pb_admin")
    password = os.getenv("DB_PASS", "Ugfhere6323tr")

    # azure sql connect tion string
    conn = 'Driver={ODBC Driver 17 for SQL Server};Server='+ server_name + ';Database=' + db_nam +';Uid=' + username +';Pwd=' + password +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    quoted = quote_plus(conn)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    logger.info("Connection string: {}".format(conn))

    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        logger.info("FUNC call")
        if executemany:
            cursor.fast_executemany = True
    logger.info("Inserting records into the table: {}".format(table_name))
    try:
        data.to_sql(table_name, engine, index=False, if_exists=mode, schema='dbo')
        logger.info("Successfully inserted {} records to SQL Server table: {}".format(data.shape[0], table_name))
    except Exception as e:
        logger.error("Exception raised while sending df to sql_server: {}, exiting".format(e))
        sys.exit(1)


def create_pd(recs, cols):
    try:
        logger.info("Total records: {}".format(recs))
        df = pd.DataFrame(recs, columns=cols)

        logger.info("Prepared DF: {}".format(df))
        return df
    except Exception as e:
        logger.error("Exception raised while preparing pandas df with "
                     "records: {} and columns: {}, Message: {}".format(recs, cols, e))
        return None
