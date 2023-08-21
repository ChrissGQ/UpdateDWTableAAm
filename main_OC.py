# -*- coding: utf-8 -*-

import gc
import pyodbc
import time
from datetime import date
from cryptography.fernet import Fernet
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser  # ver. < 3.0

chk_date = 0
key = '3rHV685mUzR93B6IRCX-86H3GMzfFYWXICzK2KPgyUE='
config = ConfigParser()


def connect_mssql():
    servName = config.get('Section_Connection', 'ServerIP')
    db_name = config.get('Section_Connection', 'DBName')
    uid = config.get('Section_Connection', 'userName')
    pw = config.get('Section_Connection', 'password')
    fernet = Fernet(key)
    pw = fernet.decrypt(pw.encode()).decode()
    uid = fernet.decrypt(uid.encode()).decode()

    try:
        print("Try Connecting to the MSSQL database...")
        conn_mssql = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};""Server="+servName+";""Database="+db_name+";""uid="+uid+";pwd="+pw)
        cursor_mssql = conn_mssql.cursor()
    except:
        conn_mssql = ""
        cursor_mssql = ""
        print("Cannot Connect MSSQL database...")
    return conn_mssql, cursor_mssql


def disconnect_mssql(conn_mssql, cursor_mssql):
    try:
        cursor_mssql.close()
        print("cursor_mssql close")
    except:
        pass
    try:
        conn_mssql.close()
        print("conn_mssql close")
    except:
        pass


def insertLOB(cursor_mssql, input_date):
    cursor_mssql.fast_executemany = True
    print('Delete data LOB')
    cursor_mssql.execute("""
    DELETE from [EMAPIDB].[dbo].[DATAPREP_EquityLOBStat]
    where UpdateTime BETWEEN '"""+input_date+""" 00:00:00.000'
    AND Dateadd(day,1,'"""+input_date+""" 00:00:00.000')
    """)
    cursor_mssql.commit()
    print('Insert data')
    cursor_mssql.execute("""
    INSERT INTO [EMAPIDB].[dbo].[DATAPREP_EquityLOBStat]
    SELECT
        MAX(UpdateTime) as UpdateTime,
        SecName,
        AVG(MDBid1Price) as Bid1Price,
        AVG(MDAsk1Price) as Ask1Price,
        SUM(MDBid1Size) as Bid1Size,
        SUM(MDAsk1Size) as Ask1Size,
        AVG((MDBid1Price + MDAsk1Price)/2) as Mid,
        AVG((MDAsk1Price - MDBid1Price)/2) as Sprd1,
        COUNT(SecName) as TickCount
        from  EMAPIDB.dbo.MDSEMAPI_EquityOrderBook
        WHERE
            UpdateTime BETWEEN '"""+input_date+""" 00:00:00.000'
               AND Dateadd(day,1,'"""+input_date+""" 00:00:00.000')
            AND MDAsk1Price between 0 AND 50000 --{MAX_PRICE}
            AND MDBid1Price between 0 AND 50000 --{MAX_PRICE}
            AND MDAsk1Price > MDBid1Price
        GROUP BY
            SecName,
            DATEPART(YEAR, UpdateTime),
            DATEPART(MONTH, UpdateTime),
            DATEPART(DAY, UpdateTime),
            (DATEPART(HOUR, UpdateTime) / 6)
        ORDER BY UpdateTime ASC
    """)
    cursor_mssql.commit()


def insert12HrLOB(cursor_mssql, input_date):
    cursor_mssql.fast_executemany = True
    print('Delete data 12HrLOB')
    cursor_mssql.execute("""
    DELETE from [EMAPIDB].[dbo].[DATAPREP_EquityLOBStat_12Hr]
    where UpdateTime BETWEEN '"""+input_date+""" 00:00:00.000'
    AND Dateadd(day,1,'"""+input_date+""" 00:00:00.000')
    """)
    cursor_mssql.commit()
    print('Insert data')
    cursor_mssql.execute("""
    INSERT INTO [EMAPIDB].[dbo].[DATAPREP_EquityLOBStat_12Hr]
    SELECT 
        MAX(UpdateTime) as UpdateTime, 
        SecName, 
        AVG(MDBid1Price) as Bid1Price, 
        AVG(MDAsk1Price) as Ask1Price, 
        SUM(MDBid1Size) as Bid1Size, 
        SUM(MDAsk1Size) as Ask1Size,
        AVG((MDBid1Price + MDAsk1Price)/2) as Mid, 
        AVG((MDAsk1Price - MDBid1Price)/2) as Sprd1, 
        COUNT(SecName) as TickCount
    from EMAPIDB.dbo.MDSEMAPI_EquityOrderBook 
    WHERE 
        UpdateTime BETWEEN '"""+input_date+""" 00:00:00.000' 
            AND Dateadd(day,1,'"""+input_date+""" 00:00:00.000')
        AND MDAsk1Price between 0 AND 50000 --{MAX_PRICE}
        AND MDBid1Price between 0 AND 50000 --{MAX_PRICE} 
        AND MDAsk1Price > MDBid1Price
    GROUP BY 
        SecName,
        DATEPART(YEAR, UpdateTime),
        DATEPART(MONTH, UpdateTime),
        DATEPART(DAY, UpdateTime),
        (DATEPART(HOUR, UpdateTime) / 12)
    ORDER BY 
        UpdateTime ASC
    """)
    cursor_mssql.commit()


def insert2HrLOB(cursor_mssql, input_date):
    cursor_mssql.fast_executemany = True
    print('Delete data 2HRLOB')
    cursor_mssql.execute("""
    DELETE from [EMAPIDB].[dbo].[DATAPREP_EquityLOBStat_2Hr]
    where UpdateTime BETWEEN '"""+input_date+""" 00:00:00.000'
    AND Dateadd(day,1,'"""+input_date+""" 00:00:00.000')
    """)
    cursor_mssql.commit()
    print('Insert data')
    cursor_mssql.execute("""
    INSERT INTO [EMAPIDB].[dbo].[DATAPREP_EquityLOBStat_2Hr]
    SELECT 
        MAX(UpdateTime) as UpdateTime, 
        SecName, 
        AVG(MDBid1Price) as Bid1Price,
        AVG(MDAsk1Price) as Ask1Price, 
        SUM(MDBid1Size) as Bid1Size, 
        SUM(MDAsk1Size) as Ask1Size,
        AVG((MDBid1Price + MDAsk1Price)/2) as Mid, 
        AVG((MDAsk1Price - MDBid1Price)/2) as Sprd1, 
        COUNT(SecName) as TickCount
    from 
        EMAPIDB.dbo.MDSEMAPI_EquityOrderBook 
    WHERE 
        UpdateTime BETWEEN '"""+input_date+""" 00:00:00.000' 
            AND Dateadd(day,1,'"""+input_date+""" 00:00:00.000')
        AND MDAsk1Price between 0 AND 50000 --{MAX_PRICE}
        AND MDBid1Price between 0 AND 50000 --{MAX_PRICE} 
        AND MDAsk1Price > MDBid1Price
        --{secname_clause}
    GROUP BY 
        SecName,
        DATEPART(YEAR, UpdateTime),
        DATEPART(MONTH, UpdateTime),
        DATEPART(DAY, UpdateTime),
        (DATEPART(HOUR, UpdateTime) / 2)
    ORDER BY 
        UpdateTime ASC
    """)
    cursor_mssql.commit()


def insertEquityOCStat(cursor_mssql, input_date):
    cursor_mssql.fast_executemany = True
    print('Delete data 2HRLOB')
    cursor_mssql.execute("""
    DELETE from [EMAPIDB].[dbo].[DATAPREP_EquityOCStat]
    where UpdateTime BETWEEN '"""+input_date+""" 00:00:00.000'
    AND Dateadd(day,1,'"""+input_date+""" 00:00:00.000')
    """)
    cursor_mssql.commit()
    print('Insert data')
    cursor_mssql.execute("""
    INSERT INTO [EMAPIDB].[dbo].[DATAPREP_EquityOCStat]
    SELECT MAX(UpdateTime) as UpdateTime, 
        SecName, 
        AVG(MDBid1Price) as Bid, 
        AVG(MDAsk1Price) as Ask, 
        AVG((MDBid1Price + MDAsk1Price)/2) as Mid,
        'O' as OpenClose
    from EMAPIDB.dbo.MDSEMAPI_EquityOrderBook 
    WHERE 
        Cast(updatetime as date) = '"""+input_date+"""' and
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
        Cast(updatetime as date) = '"""+input_date+"""' and
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


if __name__ == '__main__':
    config.read('..\config.ini')

    # read values from a section
    input_date = config.get('Section_input', 'RunDate')
    ServName = config.get('Section_Connection', 'ServerIP')
    DBName = config.get('Section_Connection', 'DBName')
    print("connecting >> "+ServName)
    print("DB >> " + DBName)
    if input_date == 'NOW':
        today = date.today()
        input_date = today.strftime("%Y-%m-%d")
    else:
        input_date = input_date[0:4]+"-"+input_date[4:6]+"-"+input_date[6:]
    print("RunDate : " + input_date)
    conn_mssql, cursor_mssql = connect_mssql()
    insertLOB(cursor_mssql, input_date)
    insert12HrLOB(cursor_mssql, input_date)
    insert2HrLOB(cursor_mssql, input_date)
    insertEquityOCStat(cursor_mssql, input_date)
    disconnect_mssql(conn_mssql, cursor_mssql)
    print("End")
