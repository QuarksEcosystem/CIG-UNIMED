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
def traduzirNomesDasColunas(session:Session,
                         database:str,
                         schema:str,
                         table:str,
                         outputTable="TabelaCigTraduzidaExportSeguros",
                         arquivoDeTraducao="streamlit\traducao\traducaoSeguros.json"
                         ):
    """
    Traduz nomes das colunas
    arquivoDeTraducao="traducao/traducaoSeguros.json"
    """
    baseTable = session.table(f"{schema}.{table}")
    dicTraducao = json.loads(open(arquivoDeTraducao,"r").read())
    renamedTable = baseTable.select([col(c).alias(dicTraducao[c]) for c in baseTable.columns if c in dicTraducao])
    renamedTable.write.mode("overwrite").saveAsTable(f"{schema}.{outputTable}")