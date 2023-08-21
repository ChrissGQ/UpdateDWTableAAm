import pandas as pd
from TFEX_Utils import TFEX_Utils
import configparser,os
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

DB_Config = configparser.ConfigParser()
DB_Config.read(os.path.join("C:\Config", "DatabaseConfig.ini"))
server_name = DB_Config["SET_EMAPIR1"]["IP"]
database_name = DB_Config["SET_EMAPIR1"]["Dbname"]
# table_name =
UserBD ="dbsupport"
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

# connection_string = "DRIVER={SQL Server};" + \
#                     f"""
#                         SERVER={server_name};
#                         DATABASE={database_name};
#                         UID={UserBD};
#                         PWD={PassBD};"""
# connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
# write_conn = create_engine(connection_url)

def query_genrate_table(input_date):

    str_date = input_date.strftime("%Y-%m-%d")
    query = f'''
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
                 UpdateTime BETWEEN '{str_date} 03:00:00.000' 
                 AND '{str_date} 9:10:00.000'
                --AND Dateadd(day,1,'2023-05-03 00:00:00.000')
                 AND MDAsk1Price between 0 AND 50000 
                 AND MDBid1Price between 0 AND 50000 
                 AND MDAsk1Price > MDBid1Price

               GROUP BY 
                 SecName,
                 DATEPART(YEAR, UpdateTime),
                 DATEPART(MONTH, UpdateTime),
                 DATEPART(DAY, UpdateTime)
                 --(DATEPART(HOUR, UpdateTime) / 6)
               ORDER BY 
                 SecName,UpdateTime ASC
    '''
    df = pd.read_sql(query, read_conn)

    return df

def query_generate_OCStats_table(input_date):

    str_date = input_date.strftime("%Y-%m-%d")
    query = f'''
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
                 UpdateTime BETWEEN '{str_date} 03:00:00.000' 
                 AND '{str_date} 9:10:00.000'
                --AND Dateadd(day,1,'2023-05-03 00:00:00.000')
                 AND MDAsk1Price between 0 AND 50000 
                 AND MDBid1Price between 0 AND 50000 
                 AND MDAsk1Price > MDBid1Price

               GROUP BY 
                 SecName,
                 DATEPART(YEAR, UpdateTime),
                 DATEPART(MONTH, UpdateTime),
                 DATEPART(DAY, UpdateTime)
                 --(DATEPART(HOUR, UpdateTime) / 6)
               ORDER BY 
                 SecName,UpdateTime ASC
    '''
    df = pd.read_sql(query, read_conn)

    return df


if __name__ == "__main__":
    mCalendar = TFEX_Utils.SETSMART_Holidays()

    trading_Day = mCalendar.get_trading_days()

    start_date = pd.Timestamp("2019-09-04")
    end_date = pd.Timestamp("2023-05-04")
    trading_Day = pd.to_datetime(trading_Day[(trading_Day>start_date) & (trading_Day<end_date)])
    # print()
    for adate in trading_Day:

        df = query_genrate_table(adate)
        print(f"writing {adate}")
        # print()

        df.to_sql(name="DATAPREP_EquityLOBStat410", con=read_conn, if_exists='append', index=False)
        print("Complete")