#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 06:33:32 2023

@author: brunobmp
"""
from datetime import datetime, timedelta
from snowflake.snowpark.functions import avg
import pandas as pd
import numpy as np
import json

## This is used to create a snowpark session
from snowflake.snowpark import Session

## Use this to create the UDF
from snowflake.snowpark.functions import udf, col
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.types import StringType

## These are necessary for the UDF function
import unidecode
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import numpy as np
import json
import sys
import re
import os


def castInvalidFormats(df : pd.DataFrame):
    df_i = df.copy()
    for c in tqdm(df.columns):
        dtype = df.dtypes[c]
        if dtype == 'O':
            df_i[c] = df[c].astype(str)
    return df_i


def uploadToSnowflake(df : pd.DataFrame,
                      session : Session,
                      outputTableName : str,
                      database : str,
                      schema : str,
                      temporary=False):
    df = castInvalidFormats(df)
    
    temp = {True: " TEMPORARY ", False:" "}[temporary]
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
                CREATE OR REPLACE{temp} TABLE {outputTableName.upper()} AS (
                SELECT * FROM "{database}"."{schema}"."{outputTableName}"
                )
                """).collect()
    print(session.sql(f"""SELECT COUNT(*) AS LineCount FROM {database}.{schema}.{outputTableName};""").collect())
    
    
    
def getSession(sessionDataFilePath="sessionData.json",
               warehouse="RAW_PROCCESS",):
    print("Starting session with",str(sessionDataFilePath))
    connection_parameters = json.loads(open(sessionDataFilePath,"r").read())
    session = Session.builder.configs(connection_parameters).create()
    print(session)

    print("Selecting warehouse...")
    session.sql(f"""USE WAREHOUSE {warehouse};""").collect()
    return session


def createSqlTableFromQuery(query : str,
                            schema : str,
                            outputTable : str, 
                            session : Session,
                            casedName=False,
                            temporary=False):
    temp = "TEMPORARY" if temporary else ""
    outputName = f'"{outputTable}"' if casedName else f'{outputTable}'
    query = f"""
    CREATE OR REPLACE {temp} VIEW "{schema}".{outputName} AS ({query});
    """
    results = session.sql(query).collect();
    return results
    
def convertFromStataTimestamp(x_time_float:float):
    return datetime(1960,1,1,0,0) + timedelta(days=x_time_float)

def getSnowflakeSession(sessionDataFilePath, session=None):
    if session is not None:
        return session
    print("Starting session with",str(sessionDataFilePath))
    connection_parameters = json.loads(open(sessionDataFilePath,"r").read())
    session = Session.builder.configs(connection_parameters).create()
    print(session)
    return session

def countTable(tabName:str, session):
    query = f"""SELECT COUNT(*) AS SIZE, '{tabName}' AS SOURCE FROM {tabName}"""
    value = pd.DataFrame(session.sql(query).collect())
    return value