# -*- coding: utf-8 -*-
"""
Gera coluna 'TIPO_ATENDIMENTO'
"""
from snowflake.snowpark.functions import avg
import pandas as pd
import numpy as np
import json
import sys

## This is used to create a snowpark session
from snowflake.snowpark import Session

## Use this to create the UDF
from snowflake.snowpark.functions import udf, col, lag, lit
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.functions import dateadd as date_add
from snowflake.snowpark.types import StringType, IntegerType
from snowflake.snowpark.types import StructField as struct
import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

## These are necessary for the UDF function
import unidecode
import sys
import re
import os


# from Decorators import TimeExecution
# from login.SnowflakeSession import SnowflakeSession

# @TimeExecution
def gerarTipoAtendimento(   session:Session,
                            database:str,
                            schema:str,
                            table:str,
                            outputTable="TipoAtendimento",
                         ):
    """
        sessionDataFilePath="sessionData.json"
        database="DEV"
        schema="RAW_SEGUROS"
        warehouse="RAW_PROCCESS"
        table="BASE_SEGUROS"
        role="RL_DEV"
        outputTable="TipoAtendimento"
    """

    # print("Selecting database, warehouse and schema...")
    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    
    df = session.table(f"{schema}.{table}")
   
    df = df.withColumn("TIPO_ATENDIMENTO",
                       F.when((col("NUM_AGR_INTER") == " ") & 
                              (col("REGIME_DOMICILIAR") == 1),
                              "REGIME DOMICILIAR")
                       .when(col("NUM_AGR_INTER") != " ","INTERNACAO")
                       .otherwise("AMBULATORIO")
                       )
    # print("Saving table with 'TIPO_ATENDIMENTO' column")
    # df.write.mode('overwrite').saveAsTable(f"{schema}.{outputTable}")
    df.createOrReplaceView(f"{schema}.{outputTable}")