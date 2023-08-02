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

# @TimeExecution
def consolidaCodigoDeCliente(session: Session,
                         schema:str,
                         table:str,
                         role:str,
                         outputTable="CodClienteCorrigido",
                         cod_cliente_colname="COD_ENTIDADE_TS_SEGURADO",
                         num_agr_inter_colname="NUM_AGR_INTER",
                         num_pedido_inter_colname="NUM_PEDIDO_INTER",
                         num_senha_inter_colname="NUM_SENHA_INTER"):
    
    
    session.sql(f"""grant USAGE, CREATE FUNCTION on schema {schema} to {role};""").collect()
    dataIsFilled = session.udf.register(
                             lambda txt: 0 if txt in {'.', ' ', None, 0, '0'} or txt is None else 1,
                             packages=["unidecode"],
                             return_type=StringType(),
                             input_types=[StringType()],
                             name="DataIsFilled",
                             replace=True,
                             stage_location="@~"
                             )

    query = f"""
    CREATE OR REPLACE TEMPORARY TABLE TEMP_COD_INTERN AS
    (
    SELECT 
         {schema}.{table}.* ,
         CASE 
              WHEN DataIsFilled({cod_cliente_colname}) = 1 THEN {cod_cliente_colname}
              WHEN DataIsFilled({cod_cliente_colname}) = 0 AND 
                   DataIsFilled({num_agr_inter_colname}) = 1 THEN {num_agr_inter_colname}
              WHEN DataIsFilled({cod_cliente_colname}) = 0 AND 
                   DataIsFilled({num_pedido_inter_colname}) = 1 THEN {num_pedido_inter_colname}
              WHEN DataIsFilled({cod_cliente_colname}) = 0 AND 
                   DataIsFilled({num_senha_inter_colname}) = 1 THEN {num_senha_inter_colname}
            ELSE '.'
        END AS TEMP_COD_INTERNACAO
     FROM {schema}.{table}
    );
    """
    results = session.sql(query).collect();
    
    ## STEP 2
    ## Criar uma tabela base para o dicionario de cod-itnernacao-id_pessoa
    query2 = f"""
    CREATE OR REPLACE TEMPORARY TABLE DIC_TEMP_COD_INTERN_BASE AS
    (SELECT DISTINCT
        {cod_cliente_colname},
        TEMP_COD_INTERNACAO,
        VLR_PROD_MEDICA
     FROM TEMP_COD_INTERN
     WHERE DataIsFilled(TEMP_COD_INTERNACAO) = 1);
    """
    results2 = session.sql(query2).collect()
    
    ## STEP 3
    ## Crie um dicionário de cod-internacao para id_pessoa
    query3= f"""
    CREATE OR REPLACE TEMPORARY TABLE DIC_TEMP_COD_INTERN AS
    (SELECT 
         {cod_cliente_colname},
         A.TEMP_COD_INTERNACAO
     FROM DIC_TEMP_COD_INTERN_BASE A
     INNER JOIN (
         SELECT TEMP_COD_INTERNACAO,
                MAX(VLR_PROD_MEDICA) AS MaxProdMedVal
         FROM DIC_TEMP_COD_INTERN_BASE GROUP BY TEMP_COD_INTERNACAO
         ) B ON A.VLR_PROD_MEDICA = B.MaxProdMedVal AND A.TEMP_COD_INTERNACAO = B.TEMP_COD_INTERNACAO
     );
    """
    results3 = session.sql(query3).collect()
    
    ## STEP 4
    ## fazer o merge de volta usando TEMP_COD_INTERNACAO para obter o ID_PESSOA
    query4= f"""
    CREATE OR REPLACE TABLE {outputTable} AS
    (SELECT 
        A.*,
        B.{cod_cliente_colname} AS ID_PESSOA
    FROM TEMP_COD_INTERN A
    LEFT JOIN (SELECT * FROM DIC_TEMP_COD_INTERN WHERE DataIsFilled(TEMP_COD_INTERNACAO) = 1) B
    ON A.TEMP_COD_INTERNACAO = B.TEMP_COD_INTERNACAO)
    """
    results4 = session.sql(query4).collect()
    
    # print("Relatório de códigos preenchidos.")
    # print("Antes do pré-processamento:")
    dataPre = pd.DataFrame(session.sql(f"""
    SELECT 
        COUNT(*) AS Total,
        COUNT(CASE WHEN COD_CLIENTE = '.' THEN 1 END) AS EmptyId,
        COUNT(CASE WHEN COD_CLIENTE <> '.' THEN 1 END) AS FilledId
    FROM {schema}.{table};
    """).collect());
    print(dataPre)
    
    # print("Após do pré-processamento:")
    data = pd.DataFrame(session.sql("""
    SELECT 
        COUNT(*) AS Total,
        COUNT(CASE WHEN ID_PESSOA IS NULL THEN 1 END) AS Empty_Id,
        COUNT(CASE WHEN ID_PESSOA IS NOT NULL THEN 1 END) AS Filled_Id
    FROM CodClienteCorrigido;
    """).collect());
    # print(data)

    # session.close()