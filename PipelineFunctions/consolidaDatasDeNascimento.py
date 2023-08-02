# -*- coding: utf-8 -*-
"""
Fixes the clients' birth date by aggregating to the highest grossing procedure value.
"""

from snowflake.snowpark.functions import avg
import json

## This is used to create a snowpark session
from snowflake.snowpark import Session

## Use this to create the UDF
from snowflake.snowpark.functions import udf, col
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.types import StringType

# from login.SnowflakeSession import SnowflakeSession
# from Decorators import TimeExecution

# @TimeExecution
def consolidaDatasDeNascimento(session:Session,
                               schema:str,
                               table:str,
                               outputTable="DtNascCorrigidas"
                            ):


    session.sql(f"""CREATE OR REPLACE TABLE {outputTable} AS
(
     SELECT 
         A.*,
         B.DT_NASCIMENTO_CORRIGIDA
     FROM {schema}.{table} A
     INNER JOIN
    (
         SELECT DISTINCT
             DT_NASCIMENTO_CLIENTE AS DT_NASCIMENTO_CORRIGIDA,
             COD_CLIENTE
         FROM (
                 (SELECT 
                      COD_CLIENTE,
                      MAX(VLR_PROD_MEDICA) AS VLR_PROD_MEDICA_MAX
                 FROM {schema}.{table}
                 GROUP BY COD_CLIENTE) C
                 INNER JOIN
                 (SELECT
                      COD_CLIENTE AS COD_CLIENTE_2,
                      VLR_PROD_MEDICA,
                      DT_NASCIMENTO_CLIENTE
                  FROM {schema}.{table}) D
             ON C.COD_CLIENTE = D.COD_CLIENTE_2 AND C.VLR_PROD_MEDICA_MAX = D.VLR_PROD_MEDICA
             )
     ) B
    ON A.COD_CLIENTE = B.COD_CLIENTE
     );""").collect()