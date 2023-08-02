# -*- coding: utf-8 -*-
"""
Remove linhas com um critério específico.
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


def removeLinhasComValores(session:Session,
                         database:str,
                         schema:str,
                         warehouse:str,
                         table:str,
                         role:str,
                         outputTable="linhasRemovidas",
                         removeColname="COD_CONTROLE",
                         removeIfValues=[819323091, 819323092, 827028229],
                         ):


    # print("Selecting database, warehouse and schema...")
    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    session.sql(f"""USE WAREHOUSE {warehouse};""").collect()
    sessionTable = session.table(f"{schema}.{table}")
    
    ## STEP 1 
    ## Criar uma coluna 'id-internação' em uma tabela temporária
    query = f"""
    CREATE OR REPLACE TABLE {outputTable} AS
    (
    SELECT 
         {schema}.{table}.* 
     FROM {schema}.{table}
     WHERE {schema}.{table}.{removeColname} NOT IN ({", ".join([str(x) for x in removeIfValues])})
    );
    """
    results = session.sql(query).collect();
    
    # session.close()