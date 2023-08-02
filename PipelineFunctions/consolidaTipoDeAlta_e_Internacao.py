# -*- coding: utf-8 -*-
"""
Traduz os nomes das colunas para o padrão final.
"""

from snowflake.snowpark.functions import avg
import snowflake.snowpark.functions as F
from snowflake.snowpark import Window
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
def consolidaTipoDeAlta_e_Internacao(session:Session,
                         database:str,
                         schema:str,
                         table:str,
                         outputTable="TipoDeAltaEinternacao"
                         ):
    """
    Consolida tipo de alta (Óbito/etc) e de internação.
    
    Exemplos de parâmetros:
        database="DEV"
        schema="RAW_SEGUROS"
        table="REGIME"
        outputTable="TipoDeAltaEinternacao"
    """
    # print("Agregando óbitos e tipos de internação...")
    baseTable = session.table(f"{schema}.{table}")
    baseTable = baseTable.withColumn("OBITO", F.when(col("TIPO_ALTA").like("%BITO%"), 1).otherwise(0))
    baseTable = baseTable.withColumn("VLR_PROD_MEDICA", F.regexp_replace("VLR_PROD_MEDICA", "\.", ""))
    baseTable = baseTable.withColumn("VLR_PROD_MEDICA", F.regexp_replace("VLR_PROD_MEDICA", "\,", "."))
    baseTable = baseTable.withColumn("VLR_PROD_MEDICA", col("VLR_PROD_MEDICA").cast("float"))
    
    a = baseTable.select("ID_INTERNACAO", "VLR_PROD_MEDICA", "TIPO_INTERNACAO")
    b = baseTable.groupBy("ID_INTERNACAO").max("VLR_PROD_MEDICA").withColumnRenamed("ID_INTERNACAO", "_ID_INTERNACAO")
    
    tipoPorInternacaoBase = a.join(b, (a["ID_INTERNACAO"] == b["_ID_INTERNACAO"]) & (a["VLR_PROD_MEDICA"] == b['"MAX(VLR_PROD_MEDICA)"']))
    tipoPorInternacao = tipoPorInternacaoBase.select("ID_INTERNACAO", "TIPO_INTERNACAO").withColumnRenamed("ID_INTERNACAO","_ID_INTERNACAO")
    tipoPorInternacao = tipoPorInternacao.dropDuplicates("_ID_INTERNACAO")
    
    
    baseTable = baseTable.withColumnRenamed("TIPO_INTERNACAO", "TIPO_INTERNACAO_ANTIGO").dropDuplicates("ID_INTERNACAO","COD_CONTROLE")
    baseTableComTipoInternacao = baseTable.join(tipoPorInternacao, (tipoPorInternacao["_ID_INTERNACAO"] == baseTable["ID_INTERNACAO"]))
    
    # print("Contando alterações:")
    counts = pd.DataFrame(baseTableComTipoInternacao.groupBy("TIPO_INTERNACAO").count().collect())
    oldCounts = pd.DataFrame(baseTableComTipoInternacao.groupBy("TIPO_INTERNACAO_ANTIGO").count().collect())
    # print(pd.concat([counts.set_index("TIPO_INTERNACAO").COUNT.rename("Novo"), 
    #                  oldCounts.set_index("TIPO_INTERNACAO_ANTIGO").COUNT.rename("Antigo")],
    #                 axis=1))
    
    tabFinal = baseTableComTipoInternacao.drop("TIPO_INTERNACAO_ANTIGO")
    # print("Salvando...")
    tabFinal.write.mode("overwrite").saveAsTable(f"{schema}.{outputTable}")
    