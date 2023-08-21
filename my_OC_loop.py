# -*- coding: utf-8 -*-

import gc
import pyodbc
import time
from datetime import date
from cryptography.fernet import Fernet
import configparser,os

import pandas as pd
from TFEX_Utils import TFEX_Utils
from sqlalchemy.engine import URL
from sqlalchemy import create_engine




# def connect_mssql():
#     servName = config.get('Section_Connection', 'ServerIP')
#     db_name = config.get('Section_Connection', 'DBName')
#     uid = config.get('Section_Connection', 'userName')
#     pw = config.get('Section_Connection', 'password')
#     fernet = Fernet(key)
#     pw = fernet.decrypt(pw.encode()).decode()
#     uid = fernet.decrypt(uid.encode()).decode()
#
#     try:
#         print("Try Connecting to the MSSQL database...")
#         conn_mssql = pyodbc.connect(
#             "Driver={SQL Server Native Client 11.0};""Server="+servName+";""Database="+db_name+";""uid="+uid+";pwd="+pw)
#         cursor_mssql = conn_mssql.cursor()
#     except:
#         conn_mssql = ""
#         cursor_mssql = ""
#         print("Cannot Connect MSSQL database...")
#     return conn_mssql, cursor_mssql
#
#
# def disconnect_mssql(conn_mssql, cursor_mssql):
#     try:
#         cursor_mssql.close()
#         print("cursor_mssql close")
#     except:
#         pass
#     try:
#         conn_mssql.close()
#         print("conn_mssql close")
#     except:
#         pass

def insertEquityOCStat(cursor_mssql, input_date):
    str_date = input_date.strftime("%Y-%m-%d")
    table_name = "DATAPREP_EquityOCStat410"
    cursor_mssql.fast_executemany = True
    print(f'Delete data on date {str_date}')
    cursor_mssql.execute("""
    DELETE from [EMAPIDB].[dbo].[DATAPREP_EquityOCStat]
    where UpdateTime BETWEEN '"""+str_date+""" 00:00:00.000'
    AND Dateadd(day,1,'"""+str_date+""" 00:00:00.000')
    """)
    cursor_mssql.commit()
    print(f'Inserting data on date: {str_date}')
    cursor_mssql.execute(f"""
    INSERT INTO [EMAPIDB].[dbo].[{table_name}]
    SELECT MAX(UpdateTime) as UpdateTime, 
        SecName, 
        AVG(MDBid1Price) as Bid, 
        AVG(MDAsk1Price) as Ask, 
        AVG((MDBid1Price + MDAsk1Price)/2) as Mid,
        'O' as OpenClose
    from EMAPIDB.dbo.MDSEMAPI_EquityOrderBook 
    WHERE 
        Cast(updatetime as date) = '"""+str_date+"""' and
        Cast(updatetime as time) between '03:05:00' and '03:35:00'
                    AND MDAsk1Price between 0 AND 50000 --{MAX_PRICE}
                    AND MDBid1Price between 0 AND 50000 --{MAX_PRICE} 
                    AND MDAsk1Price > MDBid1Price
    GROUP BY 
        SecName,
        DATEPART(YEAR, UpdateTime),
        DATEPART(MONTH, UpdateTime),
        DATEPART(DAY, UpdateTime)
    UNION
    SELECT MAX(UpdateTime) as UpdateTime, 
        SecName, 
        AVG(MDBid1Price) as Bid, 
        AVG(MDAsk1Price) as Ask, 
        AVG((MDBid1Price + MDAsk1Price)/2) as Mid,
        'C' as OpenClose
    from EMAPIDB.dbo.MDSEMAPI_EquityOrderBook 
    WHERE 
        Cast(updatetime as date) = '"""+str_date+"""' and
        Cast(updatetime as time) between '09:00:00' and '09:10:00'
                    AND MDAsk1Price between 0 AND 50000 --{MAX_PRICE}
                    AND MDBid1Price between 0 AND 50000 --{MAX_PRICE} 
                    AND MDAsk1Price > MDBid1Price
    GROUP BY 
            SecName,
            DATEPART(YEAR, UpdateTime),
            DATEPART(MONTH, UpdateTime),
            DATEPART(DAY, UpdateTime)
    ORDER BY UpdateTime ASC
    """)
    cursor_mssql.commit()

def insertEquityOCStat_OLD(cursor_mssql, input_date):
    str_date = input_date.strftime("%Y-%m-%d")
    table_name = "DATAPREP_EquityOCStat"
    cursor_mssql.fast_executemany = True
    print(f'Delete data on date {str_date}')
    cursor_mssql.execute(f"""
    DELETE from [EMAPIDB].[dbo].[{table_name}]
    where UpdateTime BETWEEN '"""+str_date+""" 00:00:00.000'
    AND Dateadd(day,1,'"""+str_date+""" 00:00:00.000')
    """)
    cursor_mssql.commit()
    print(f'Inserting data on date: {str_date}')
    cursor_mssql.execute(f"""
    INSERT INTO [EMAPIDB].[dbo].[{table_name}]
    SELECT MAX(UpdateTime) as UpdateTime, 
        SecName, 
        AVG(MDBid1Price) as Bid, 
        AVG(MDAsk1Price) as Ask, 
        AVG((MDBid1Price + MDAsk1Price)/2) as Mid,
        'O' as OpenClose
    from EMAPIDB.dbo.MDSEMAPI_EquityOrderBook 
    WHERE 
        Cast(updatetime as date) = '"""+str_date+"""' and
        Cast(updatetime as time) between '03:05:00' and '03:35:00'
                    AND MDAsk1Price between 0 AND 50000 --{MAX_PRICE}
                    AND MDBid1Price between 0 AND 50000 --{MAX_PRICE} 
                    AND MDAsk1Price > MDBid1Price
    GROUP BY 
        SecName,
        DATEPART(YEAR, UpdateTime),
        DATEPART(MONTH, UpdateTime),
        DATEPART(DAY, UpdateTime)
    UNION
    SELECT MAX(UpdateTime) as UpdateTime, 
        SecName, 
        AVG(MDBid1Price) as Bid, 
        AVG(MDAsk1Price) as Ask, 
        AVG((MDBid1Price + MDAsk1Price)/2) as Mid,
        'C' as OpenClose
    from EMAPIDB.dbo.MDSEMAPI_EquityOrderBook 
    WHERE 
        Cast(updatetime as date) = '"""+str_date+"""' and
        Cast(updatetime as time) between '09:00:00' and '09:30:00'
                    AND MDAsk1Price between 0 AND 50000 --{MAX_PRICE}
                    AND MDBid1Price between 0 AND 50000 --{MAX_PRICE} 
                    AND MDAsk1Price > MDBid1Price
    GROUP BY 
            SecName,
            DATEPART(YEAR, UpdateTime),
            DATEPART(MONTH, UpdateTime),
            DATEPART(DAY, UpdateTime)
    ORDER BY UpdateTime ASC
    """)
    cursor_mssql.commit()


from sqlalchemy.orm import sessionmaker
DB_Config = configparser.ConfigParser()
DB_Config.read(os.path.join("C:\Config", "DatabaseConfig.ini"))
server_name = DB_Config["SET_EMAPIR1"]["IP"]
database_name = DB_Config["SET_EMAPIR1"]["Dbname"]
# table_name =
UserBD = "dbsupport"
PassBD = "Support@DB"


connection_string = "DRIVER={SQL Server};" + \
                    f"""
                        SERVER={server_name};
                        DATABASE={database_name};
                        UID={UserBD};
                        PWD={PassBD};"""
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
# global conn
read_conn = create_engine(connection_url)
Session = sessionmaker(bind=read_conn)
session = Session()
if __name__ == "__main__":


    mCalendar = TFEX_Utils.SETSMART_Holidays()
    trading_Day = mCalendar.get_trading_days()

    start_date = pd.Timestamp("2019-09-04")
    end_date = pd.Timestamp("2023-05-04")
    trading_Day = pd.to_datetime(trading_Day[(trading_Day>start_date) & (trading_Day<end_date)])
    # print()



    for adate in trading_Day:
        # pass
        # with read_conn.connect() as cursor_mssql:
        # insertEquityOCStat(session,adate)
        insertEquityOCStat_OLD(session,adate)

