# -*- coding: utf-8 -*-
"""
Replace special charecters in a snowflake table using a Snowpark UDF.
"""

from snowflake.snowpark.functions import avg
import unicodedata
import json

## This is used to create a snowpark session
from snowflake.snowpark import Session

## Use this to create the UDF
from snowflake.snowpark.functions import udf, col, upper
from snowflake.snowpark.functions import regexp_replace
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.types import StringType

## These are necessary for the UDF function
from tqdm import tqdm
import unidecode
import sys
import re
import os

# from Decorators import TimeExecution
from PipelineFunctions.dicionarioCaracteresEspeciais import *
# from login.SnowflakeSession import SnowflakeSession



# @TimeExecution
def trataCaracteresEspeciais1(session:Session,
                             database:str,
                             schema:str,
                             table:str,
                             outputTable="SemCaracteresEspeciais",
                             columns=['DS_PROD_MEDICA', 'TIPO_TABELA']):

    # print("Selecting database, warehouse and schema...")
    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    df = session.table(f"{schema}.{table}")

    for c in tqdm(columns):
        df = df.withColumn(c, upper(c))
        for p in replace_pairs:
            df = df.withColumn(c, regexp_replace(c, p[0], p[1]))
    
    # df.write.mode("overwrite").saveAsTable(f'{schema}.{outputTable}')
    df.createOrReplaceView(f"{schema}.{outputTable}")
    