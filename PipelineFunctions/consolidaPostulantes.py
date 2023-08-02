# -*- coding: utf-8 -*-
"""
Consolida códigos de cliente vazios usando ids de internação e de guias.
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

# from login.SnowflakeSession import SnowflakeSession
# from Decorators import TimeExecution
import Utils

# @TimeExecution
def consolidaPostulantes(session:Session,
                         database:str,
                         schema:str,
                         table:str,
                         outputTable="Postulantes"):
    """
    Consolida postulantes (Tópico 12)

    database="DEV"
    schema="RAW_SEGUROS"
    table="PADR_CBHPM"
    outputTable="Postulantes"
    
    """
    
    tableColumns = session.table(f"{schema}.{table}").columns
    ValuesQuery = f"""
    SELECT DISTINCT
        COD_CONTROLE2,
        CONCAT(COD_CONTROLE2, CAST(ID_GRUPO AS varchar), '-', CAST(CODIGO_ESTIPULANTE AS varchar), '-', CAST(NR_CONTRATO AS varchar)) AS KEY,
        ID_GRUPO,
        NR_CONTRATO AS NR_ESTIPULANTE,
        NR_CONTRATO
    FROM {schema}.{table}
    """
    Utils.createSqlTableFromQuery(ValuesQuery, schema, "TAB_POSTULANTES_RAW", session, temporary=True)
    
    MaxValuesQuery = """
    SELECT 
        COUNT(*) AS N_TIMES,
        KEY
    FROM TAB_POSTULANTES_RAW 
    WHERE KEY IS NOT NULL
    GROUP BY KEY
    """
    Utils.createSqlTableFromQuery(MaxValuesQuery, schema, "TAB_POSTULANTES_MAX", session, temporary=True)
    
    MostCommonLines = """
    SELECT DISTINCT
        COD_CONTROLE2,
        ID_GRUPO,
        NR_ESTIPULANTE,
        NR_CONTRATO
    FROM TAB_POSTULANTES_RAW a
    INNER JOIN TAB_POSTULANTES_MAX b ON a.KEY = b.KEY
    WHERE COD_CONTROLE2 IS NOT NULL AND COD_CONTROLE2 <> '' AND COD_CONTROLE2 <> ' '
    """
    Utils.createSqlTableFromQuery(MostCommonLines, schema, "TAB_POSTULANTES", session, temporary=True)
    
    MostCommonLinesWithMaxCols = """
    SELECT DISTINCT
        COD_CONTROLE2,
        MAX(ID_GRUPO) AS ID_GRUPO,
        MAX(NR_ESTIPULANTE) AS NR_ESTIPULANTE,
        MAX(NR_CONTRATO) AS NR_CONTRATO
    FROM TAB_POSTULANTES GROUP BY  COD_CONTROLE2
    """
    Utils.createSqlTableFromQuery(MostCommonLinesWithMaxCols, schema, "DICIONARIO_TOPICO_12", session, temporary=True)
    
    usedCols = sorted(list(set(tableColumns) - set(["ID_GRUPO", "NR_ESTIPULANTE", "NR_CONTRATO"])))
    usedColsExpr = ',\n\t'.join(["a." + x for x in usedCols])
    generateTableQuery = f"""
    SELECT 
        {usedColsExpr},
        CASE 
            WHEN b.ID_GRUPO = '.' THEN -1
            ELSE b.ID_GRUPO
        END AS ID_GRUPO,
        b.NR_ESTIPULANTE,
        b.NR_CONTRATO
    FROM {schema}.{table} a
    LEFT JOIN DICIONARIO_TOPICO_12 b
    ON a.COD_CONTROLE2 = b.COD_CONTROLE2
    """
    # print("Saving...")
    Utils.createSqlTableFromQuery(generateTableQuery, schema, outputTable, session, temporary=False)
    
    
    
    
    
    