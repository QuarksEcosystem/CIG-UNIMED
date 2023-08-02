# -*- coding: utf-8 -*-
"""
Consolida dados de reinternação em menos de 30 dias. 
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
def consolidaReinternacoes(session:Session,
                         database:str,
                         schema:str,
                         table:str,
                         outputTable="dadosTratados",
                         ):
    """
    Requer os resultados da função
    consolidaInternacoes(
            schema=schema,
            table=outputTable,
            outputTable=outputTable,
            cod_cliente_colname="ID_PESSOA")
    
    table='tabCiGSnowflake'
    """
    internacoes = session.table(table).select("ID_INTERNACAO","ID_PESSOA","DT_INTERNACAO","DT_ALTA"
                                              ).distinct(
                                              ).orderBy(col("ID_PESSOA").asc(),
                                                        col("ID_INTERNACAO").asc())

    lagWindow = Window.partitionBy("ID_PESSOA").orderBy(col("ID_PESSOA").asc(),
                                                        col("ID_INTERNACAO").asc(),
                                                        col("DT_ALTA").desc())

    internacoes = internacoes.withColumn("DT_ALTA_ULTIMA_INTERNACAO", F.lag(F.col("DT_ALTA"), 1).over(lagWindow))
    internacoes = internacoes.withColumn("ID_PESSOA_ULTIMA_INTERNACAO", F.lag(F.col("ID_PESSOA"), 1).over(lagWindow))
    
    internacoes = internacoes.withColumn("TEMPO_DESDE_ULTIMA_INTERNACAO",
                                         F.abs(F.datediff("day", col("DT_INTERNACAO"), col("DT_ALTA_ULTIMA_INTERNACAO"))))
    internacoes = internacoes.withColumn("TEMPO_DESDE_ULTIMA_INTERNACAO",
                                         F.when(col("ID_PESSOA") != col("ID_PESSOA_ULTIMA_INTERNACAO"),
                                                None).otherwise(col("TEMPO_DESDE_ULTIMA_INTERNACAO")))
    
    #a = pd.DataFrame(internacoes.take(300)); a.sort_values(by=["TEMPO_DESDE_ULTIMA_INTERNACAO"]).dropna()
    baseCig = session.table(table)
    baseCigComColunaReint = baseCig.join(internacoes.select(col("ID_INTERNACAO"),col("TEMPO_DESDE_ULTIMA_INTERNACAO")), on="ID_INTERNACAO")
    
    baseCigComColunaReint.write.mode("overwrite").saveAsTable(f"{schema}.{outputTable}")

