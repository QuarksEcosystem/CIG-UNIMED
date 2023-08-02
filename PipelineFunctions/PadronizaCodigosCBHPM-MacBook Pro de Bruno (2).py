# -*- coding: utf-8 -*-
"""
Tópico 11 - padronizando códigos CBHPM
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
from pathlib import Path
from tqdm import tqdm
import unidecode
import sys
import re
import os

import Utils

def uploadArquivosStataParaSnowflake(root : Path,
                                     session : Session,
                                    sessionDataFilePath="sessionData.json",
                                    database="DEV",
                                    schema="TAB_APOIO",
                                    warehouse="RAW_PROCCESS",
                                    table="BASE_SEGUROS",
                                    role="RL_DEV",):
    print(session.sql(f"""CREATE SCHEMA IF NOT EXISTS {schema};""").collect())
    sources = []
    for f in tqdm(os.listdir(root)):
        df = pd.read_stata(root/f)
        df_c = Utils.castInvalidFormats(df)
        tabName = re.sub("[^A-Za-z0-9]","_", f.split(".")[0]).upper()
        sources.append(tabName)
        print("Uploading",tabName,"...")
        print(Utils.uploadToSnowflake(df_c, session, tabName, database=database, schema=schema))
    print("Upload completo, tabelas:",sources)


def criaTabelaDeApoio(
        session : Session,
        schema="TAB_APOIO",
        database="DEV",
        outputTable="TabelaDeApoio",
        fontes=['AMB90',
                'AMB92',
                'AMB96',
                'AMB99',
                'CBHPM_3',
                'CBHPM_4',
                'CBHPM_5']):
    
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    
    with open("sql/criarTabelaDeApoio.sql","r") as f:
        partialQuery = f.read()    
    combinedTableQuery = "\nUNION\n".join([partialQuery.format(f, schema, f) for f in fontes])
    tabelComPorteAnest = f"""
    SELECT 
        *
    FROM ({combinedTableQuery}) COD_CBHPM
    LEFT JOIN 
        (SELECT * FROM "{schema}"."PORTE_ANESTESICO_VIDAS_3") PORTE_ANEST
    ON PORTE_ANEST."cod_item_prod_medica_VIDAS2" = COD_CBHPM."cdigotuss_90"
"""
    print("Criando tabela de apoio...")
    result = Utils.createSqlTableFromQuery(tabelComPorteAnest, schema, "TAB_APOIO_COM_PORTE_ANEST", session)
    print(result)
    
           


def PadronizaCodigosCBHPM(sessionDataFilePath="sessionData.json",
                         database="DEV",
                         schema="RAW_SEGUROS",
                         warehouse="RAW_PROCCESS",
                         table="BASE_SEGUROS",
                         role="RL_DEV",
                         outputTable="CodigosPadronizados",
                         schemaTabApoio="TAB_APOIO",
                         fontesTabApoio=['AMB90',
                                         'AMB92',
                                         'AMB96',
                                         'AMB99',
                                         'CBHPM_3',
                                         'CBHPM_4',
                                         'CBHPM_5']
                         ):
    session = Utils.getSession(sessionDataFilePath, warehouse)

    print("Selecting database, warehouse and schema...")
    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    sessionTable = session.table(f"{schema}.{table}")
    
    ## STEP 1 
    ## Criar uma tabela de apoio.
    criaTabelaDeApoio(session,
                      schema=schemaTabApoio,
                      database=database,
                      outputTable="TabelaDeApoio",
                      fontes=fontesTabApoio)
    
    ## STEP 2
    ## merge da tabela de apoio para a base de internações.
    
    session.close()