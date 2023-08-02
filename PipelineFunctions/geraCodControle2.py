# -*- coding: utf-8 -*-
"""
O ID_PESSOA é obtido da seguinte forma:
    COD_ENTIDADE_TS_SEGURADO se COD_ENTIDADE_TS_SEGURADO for <> -1 e <> "."
    NUM_AGR_INTER em caso contrário
    
A coluna ID_PESSOA é gerada assim:

    gen cod_cliente_revisado = cod_entidade_ts_segurado
    replace cod_cliente_revisado = cod_entidade_ts_segurado_3 if merge_cliente3 == 3 & (cod_entidade_ts_segurado == -1 | cod_entidade_ts_segurado == .)
    replace cod_cliente_revisado = cod_entidade_ts_segurado_4 if merge_cliente4 == 3 & (cod_entidade_ts_segurado == -1 | cod_entidade_ts_segurado == .)
    tostring cod_cliente_revisado, replace
    replace cod_cliente_revisado = num_agr_inter if cod_cliente_revisado == "-1"
    replace cod_cliente_revisado = num_agr_inter if cod_cliente_revisado == " "
    replace cod_cliente_revisado = num_agr_inter if cod_cliente_revisado == ""
    rename cod_cliente_revisado ID_PESSOA
    
Também é preciso criar essas identificações:
    gen tipo_identificacao_cliente = "ORIGINAL DA SEGUROS"
    replace  tipo_identificacao_cliente = "IDENTIFICADO PELO Nº DO AGRUPAMENTO" if merge_cliente2 == 3 & (cod_entidade_ts_segurado == -1 | cod_entidade_ts_segurado == .)
    replace  tipo_identificacao_cliente = "IDENTIFICADO PELO Nº DO PEDIDO" if merge_cliente3 == 3 & (cod_entidade_ts_segurado == -1 | cod_entidade_ts_segurado == .)
    replace  tipo_identificacao_cliente = "IDENTIFICADO PELO Nº DA SENHA" if merge_cliente4 == 3 & (cod_entidade_ts_segurado == -1 | cod_entidade_ts_segurado == .)
    replace  tipo_identificacao_cliente = "CRIADO PELO Nº DO AGRUPAMENTO DA INTERNAÇÃO" if cod_cliente_revisado == num_agr_inter
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

## Custom code
# from login.SnowflakeSession import SnowflakeSession
# from Decorators import TimeExecution
import Utils

# @TimeExecution
def geraCodControle2(session:Session,
                 table:str,
                 schema:str,
                 outputTable="InternacoesAgregadas",
                 dt_referencia="DT_OCORR_EVENTO"):
    """
        Gera a coluna 'COD_CONTROLE2' unificando o primeiro COD_CONTROLE de cada internação
    """
    # print("Generating column 'ID_PESSOA'...")
    
    cod_controle_2Query = f"""
    SELECT 
        INTERNACOES.*,
        COD_CONTROLE2
    FROM {schema}.{table} INTERNACOES
    LEFT JOIN
        (SELECT
            COD_CONTROLE AS COD_CONTROLE2,
            ID_INTERNACAO
        FROM
            (
                (SELECT 
                    ROW_NUMBER() OVER(PARTITION BY ID_INTERNACAO ORDER BY COD_CONTROLE ASC) AS ROW_NUMBER,
                    COD_CONTROLE,
                    {dt_referencia},
                    ID_INTERNACAO
                FROM {schema}.{table}) BASE
                    INNER JOIN
                (SELECT 
                     ID_INTERNACAO AS M_ID_INTERNACAO,
                     MIN(TO_DATE({dt_referencia})) AS DT_EVENTO
                FROM {schema}.{table}
                GROUP BY ID_INTERNACAO) PRIMEIRA
                
                ON PRIMEIRA.DT_EVENTO = BASE.{dt_referencia} AND PRIMEIRA.M_ID_INTERNACAO = BASE.ID_INTERNACAO
            )
            WHERE ROW_NUMBER = 1) AGG_CD_CONTROLE_2
    ON INTERNACOES.ID_INTERNACAO = AGG_CD_CONTROLE_2.ID_INTERNACAO
    """
    Utils.createSqlTableFromQuery(cod_controle_2Query, schema, outputTable, session)

    