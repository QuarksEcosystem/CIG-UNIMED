from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import json
from tqdm import tqdm
from snowflake.connector.pandas_tools import write_pandas

def connection(sessionDataFilePath:json):
    session = Session.builder.configs(sessionDataFilePath).create()
    session.sql(f"""USE WAREHOUSE WH_POC;""").collect()
    session.sql(f"""USE DATABASE UNIMED_STREAMLIT_SF""").collect()
    session.sql(f"""USE SCHEMA BLOB;""").collect()
    return session

def connection_report(sessionDataFilePath:json):
    session = Session.builder.configs(sessionDataFilePath).create()
    session.sql(f"""USE WAREHOUSE WH_POC;""").collect()
    session.sql(f"""USE DATABASE UNIMED_STREAMLIT_SF""").collect()
    session.sql(f"""USE SCHEMA STATA;""").collect()
    return session

def castInvalidFormats(df: pd.DataFrame):
    df_i = df.copy()
    for c in df.columns:
        dtype = df.dtypes[c]
        if dtype == 'object':
            df_i[c] = df[c].astype(str)
    return df_i

def uploadToSnowflake(df: pd.DataFrame,
                      session: Session,
                      outputTableName: str,
                      database: str,
                      schema: str,
                      temporary=False):

    
    # Aplicar a função castInvalidFormats() no DataFrame completo
    df = castInvalidFormats(df)

    temp = {True: " TRANSIENT ", False:" "}[temporary]
    dtypeDict = {"str":"varchar(255)",
                 "int64":"int",
                 "int32":"int",
                 "int16":"int",
                 "int8":"int",
                 "float64":"float",
                 "float32":"float",
                 "object":"varchar(255)"}
    coNamesPre = [f"{x} {dtypeDict[str(y)]}," for x, y in zip(df.columns, df.dtypes)]
    coNamesPre[-1] = coNamesPre[-1].replace(",","")
    colNames = "\n".join(coNamesPre)
        
    SparkDf = session.write_pandas(df,
                                   f"{outputTableName}",
                                   overwrite=True)
    # print(colNames)
    session.sql(f"""
                CREATE OR REPLACE{temp} TABLE {outputTableName.upper()} AS (
                SELECT * FROM "{database}"."{schema}"."{outputTableName}"
                )
                """).collect()
    print("done saving !")
    print("Total records:")
    print(session.sql(f"""SELECT COUNT(*) AS LineCount FROM {database}.{schema}.{outputTableName};""").collect())
    
def updateUserHistory(session: Session,
                      outputTableName: str,
                      database: str,
                      schema: str,
                      email,
                      cnpj,
                      nome,
                      telefone
                      ):
    session.sql(f"""
                INSERT INTO {database}.{schema}.{outputTableName.upper()} VALUES ('{email}', 
                                                                '{cnpj}',
                                                                '{nome}',
                                                                '{telefone}',
                                                                CURRENT_TIMESTAMP())
                """).collect()

    
    

    
def verif_insert_table(df : pd.DataFrame,
                      session : Session,
                      outputTableName : str,
                      database : str,
                      schema : str,
                      temporary=False):
    
    result = session.sql(f"""SELECT COUNT(*) FROM {outputTableName}""").collect()[0][0]
    if int(result) > 0:
        query = f"INSERT INTO {outputTableName} SELECT * FROM VALUES {tuple(df.iloc[0])} ORDER BY ROWID DESC"
        session.sql(query).collect()
        print("done saving !")
    else:
        df = castInvalidFormats(df)
    
        temp = {True: " TRANSIENT ", False:" "}[temporary]
        dtypeDict = {"str":"varchar(255)",
                    "int64":"int",
                    "int32":"int",
                    "int16":"int",
                    "int8":"int",
                    "float64":"float",
                    "float32":"float",
                    "object":"varchar(255)"}
        coNamesPre = [f"{x} {dtypeDict[str(y)]}," for x, y in zip(df.columns, df.dtypes)]
        coNamesPre[-1] = coNamesPre[-1].replace(",","")
        colNames = "\n".join(coNamesPre)
        SparkDf = session.write_pandas(df,
                                   f"{outputTableName}",
                                   overwrite=True)
        session.sql(f"""
                CREATE OR REPLACE{temp}TABLE {outputTableName.upper()} AS (
                SELECT * FROM "{database}"."{schema}"."{outputTableName}"
                )
                """).collect()
        print("done saving !")
        print("Total records:")
        print(session.sql(f"""SELECT COUNT(*) AS LineCount FROM {database}.{schema}.{outputTableName};""").collect())
        
def consulta_snow(session : Session):
    query = f"""SELECT * FROM UNIMED_STREAMLIT_SF.STATA.AMOSTRA_RETORNO LIMIT 10"""
    value = pd.DataFrame(session.sql(query).collect())

    return value

def send_email_to_unimed(session : Session):
    send_email = session.call("SEND_EMAIL_NOTIFICATION")
    return send_email