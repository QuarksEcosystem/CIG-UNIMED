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
def geraIdPessoa(session:Session,
                 table:str,
                 schema:str,
                 outputTable="baseComIdPessoa",
                 colunaAlvo="COD_ENTIDADE_TS_SEGURADO",
                 hierarquia=[
                     "COD_ENTIDADE_TS_SEGURADO",
                     "NUM_AGR_INTER",
                     "NUM_PEDIDO_INTER",
                     "COD_CONTROLE"]):
    """
        A 'colunaAlvo', por padrão 'COD_ENTIDADE_TS_SEGURADO',
        será a base para gerar a coluna ID_PESSOA.
    """
    # print("Generating column 'ID_PESSOA'...")
    baseQuery = """
    SELECT DISTINCT
        {} AS ID_PESSOA, 
        {} AS CHAVE,
        '{}' AS PRIORIDADE
    FROM {}.{} 
    WHERE ID_PESSOA IS NOT NULL AND ID_PESSOA <> '.' AND ID_PESSOA <> ''
    AND CHAVE IS NOT NULL AND CHAVE <> '.' AND CHAVE <> ''
    """
    dictQuery = "UNION".join([baseQuery.format(colunaAlvo, x, i, schema, table) for i,x in enumerate(hierarquia)])
    
    mappingQuery = f"""
    SELECT 
        BASE.*
    FROM ({dictQuery}) BASE
    INNER JOIN 
    (SELECT ID_PESSOA, MIN(PRIORIDADE) AS PRIORIDADE FROM ({dictQuery}) GROUP BY ID_PESSOA) MIN_VALUES
    ON BASE.PRIORIDADE = MIN_VALUES.PRIORIDADE AND BASE.ID_PESSOA = MIN_VALUES.ID_PESSOA
    """
    
    if colunaAlvo not in hierarquia:
        hierarquia = [colunaAlvo] + hierarquia
    caseStatementBase = """WHEN {} IS NOT NULL AND {} <> '.' AND {} <> '' THEN {}"""
    caseStatement = "\n".join([caseStatementBase.format(x,x,x,x) for x in hierarquia])
    
    rawDatasetWithKeyColumnQuery = f"""
    SELECT 
        BASE.*,
        MAPPING.ID_PESSOA
    FROM 
        (SELECT 
            *,
            CASE 
                {caseStatement}
                ELSE NULL
            END AS CHAVE
        FROM {schema}.{table}) BASE
    LEFT JOIN 
        (SELECT CHAVE, ID_PESSOA FROM ({mappingQuery})) MAPPING
    ON BASE.CHAVE = MAPPING.CHAVE
    """
    
    Utils.createSqlTableFromQuery(rawDatasetWithKeyColumnQuery,
                                  schema=schema,
                                  outputTable=outputTable, 
                                  session=session)
    
    # print("Relatório de códigos preenchidos.")
    # print("Antes do pré-processamento:")
    # dataPre = pd.DataFrame(session.sql(f"""
    # SELECT 
    #     COUNT(*) AS Total,
    #     COUNT(CASE WHEN {colunaAlvo} = '.' THEN 1 END) AS EmptyId,
    #     COUNT(CASE WHEN {colunaAlvo} <> '.' THEN 1 END) AS FilledId
    # FROM {schema}.{table};
    # """).collect());
    # # print(dataPre)
    
    # # print("Após do pré-processamento:")
    # data = pd.DataFrame(session.sql(f"""
    # SELECT 
    #     COUNT(*) AS Total,
    #     COUNT(CASE WHEN ID_PESSOA IS NULL THEN 1 END) AS Empty_Id,
    #     COUNT(CASE WHEN ID_PESSOA IS NOT NULL THEN 1 END) AS Filled_Id
    # FROM {schema}.{outputTable};
    # """).collect());
    # print(data)
