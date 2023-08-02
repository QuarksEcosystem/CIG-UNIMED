# -*- coding: utf-8 -*-
"""
Conta quantidade de linhas na base.
"""
## This is used to create a snowpark session
# from login.SnowflakeSession import SnowflakeSession
from snowflake.snowpark import Session
# from Decorators import TimeExecution
import json
## These are necessary for the UDF function

# @TimeExecution
def ContarLinhas(   session:Session,
                    database:str,
                    schema:str,
                    table:str):
    """
        Conta a quantidade de linhas na base alvo.
    """

    # print("Selecting database, warehouse and schema...")
    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    
    r = session.sql(f"""SELECT COUNT(*) AS QTD_LINHAS FROM {schema}.{table}; """).collect()
    return r

