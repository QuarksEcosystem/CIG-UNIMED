# -*- coding: utf-8 -*-
"""
Genericamente consolida uma coluna baseado no valor (maximo ou minimo) de outra coluna

É usado para consolidar, por exemplo, dados de rede:
    - INTERCAMBIO
    - REDE DIRETA 
    - REEMBOLSO

"""

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
import sys
import re
import os

# from Decorators import TimeExecution
# from login.SnowflakeSession import SnowflakeSession

# @TimeExecution
def consolidaDadosPorValor(session:Session,
                         database:str,
                         schema:str,
                         table:str,
                         outputTable="dadosTratados",
                         aggregateColumn="TP_REDE",
                         aggregationUnit="COD_CLIENTE",
                         valueColumn="VLR_PROD_MEDICA",
                         aggType="MAX", # 'MAX' or 'MIN',
                         replaceOriginalColname=True
                         ):

    # print("Selecting database, warehouse and schema...")
    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    
    aggFunc = {"MAX":"MAX", 
               "max":"MAX",
               "maximum":"MAX",
               "MIN":"MIN",
               "min":"MIN",
               "minimum":"MIN"}[aggType]

    
    resumoAntes = session.sql(f"""
    SELECT 
        {aggregateColumn},
        SUM(CAST(REPLACE(REPLACE({valueColumn}, '.',''), ',', '.') AS float)) AS {valueColumn}
    FROM {schema}.{table}
    GROUP BY {aggregateColumn}""").collect()
    
    session.sql(f"DROP TABLE IF EXISTS {schema}.{outputTable}")
    session.sql(f"""CREATE OR REPLACE TABLE {outputTable} AS
   (
     SELECT DISTINCT
         A.*,
         B.{aggregateColumn}_CORRIGIDA
     FROM {schema}.{table} A
     INNER JOIN
    (
         SELECT DISTINCT
             {aggregateColumn} AS {aggregateColumn}_CORRIGIDA,
             {aggregationUnit}
         FROM (
                 (SELECT 
                      {aggregationUnit},
                      {aggFunc}(CAST(REPLACE(REPLACE({valueColumn}, '.',''), ',', '.') AS float)
                                ) AS {valueColumn}_{aggFunc}
                 FROM {schema}.{table}
                 GROUP BY {aggregationUnit}) C
                 INNER JOIN
                 (SELECT
                      {aggregationUnit} AS {aggregationUnit}_2,
                      CAST(REPLACE(REPLACE({valueColumn}, '.',''), ',', '.') AS float) AS {valueColumn},
                      {aggregateColumn}
                  FROM {schema}.{table}) D
             ON C.{aggregationUnit} = D.{aggregationUnit}_2 AND C.{valueColumn}_{aggFunc} = D.{valueColumn}
             )
     ) B
    ON A.{aggregationUnit} = B.{aggregationUnit}
     );""").collect()

    changeLog = session.sql(f"""
                SELECT 
                    SUM(CAST({aggregateColumn}_CORRIGIDA <> {aggregateColumn} AS int)) AS Total_Changed_Records
                FROM {outputTable};
                            """).collect()
    # print("Changed records:\n",pd.DataFrame(changeLog).loc[0].to_dict())

    resumoDepois = session.sql(f"""
    SELECT 
        {aggregateColumn}_CORRIGIDA,
        SUM(CAST(REPLACE(REPLACE({valueColumn}, '.',''), ',', '.') AS float)) AS {valueColumn}
    FROM {schema}.{outputTable}
    GROUP BY {aggregateColumn}_CORRIGIDA""").collect()
   
    # print("Before correction:")
    # print(pd.DataFrame(resumoAntes))
    
    # print("Before correction:")
    # print(pd.DataFrame(resumoDepois))
    
    if replaceOriginalColname:
        session.sql(f"""
                    ALTER TABLE {schema}.{outputTable}
                    DROP COLUMN {aggregateColumn};
                    """).collect()
        session.sql(f"""
                    ALTER TABLE {schema}.{outputTable}
                    RENAME COLUMN {aggregateColumn}_CORRIGIDA TO {aggregateColumn};
                    """).collect()