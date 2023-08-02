# -*- coding: utf-8 -*-
"""
Trata CIDs faltantes na base
"""
## This is used to create a snowpark session
# from login.SnowflakeSession import SnowflakeSession
from snowflake.snowpark import functions as F
from snowflake.snowpark import Session
from snowflake.snowpark import Window
# from Decorators import TimeExecution
from tqdm import tqdm
import pandas as pd
import Utils
import json



# @TimeExecution
def trataCids(  session:Session,
                database:str,
                schema:str,
                table:str,
                outputTable="CidsUnificados",
                colVlr="VLR_PROD_MEDICA",
                aggCol="COD_CONTROLE2",
                colunasCid=["CID_SAIDA",
                            "CID_PRINC_ENTRADA",
                            "CID_SEC_ENTRADA",
                                ]):
    """
        Preenche com CID único para cada internação.
        Os critérios são : 
            - CID de saída de maior custo
            - CID Principal de maior custo
            - CID Secundário de maior custo
            
        Exemplos de parâmetros:
            database="DEV"
            schema="RAW_SEGUROS"
            table="tabCiGSnowflake"
            
            outputTable="tabCiGSnowflake"
            
            colVlr="VLR_PROD_MEDICA"
            aggCol="COD_CONTROLE2"
            colunasCid=["CID_SAIDA",
                        "CID_PRINC_ENTRADA",
                        "CID_SEC_ENTRADA",
                            ]
    """
    
    # print("Checando colunas pré-existentes...")
    try:
        session.sql(f"""ALTER VIEW {schema}.{table} DROP COLUMN CID""").collect()
    except:
        print("Não foi preciso excluir a coluna CID pois ela não existe.")
        pass
    
    # print("Construindo dicionario de CID por internação...")
    finalQuery_L = []
    for i, c in enumerate(tqdm(colunasCid)):
        baseQuery = f"""
        (
            SELECT A.* FROM 
            (SELECT 
                {colVlr},
                {aggCol},
                {c} AS CID,
                {i} AS PRIORIDADE
            FROM {schema}.{table} 
            HAVING {c} <> '' AND {c} <> ' ' AND {c} IS NOT NULL ) A
            INNER JOIN 
            (
                SELECT 
                    {aggCol},
                    MAX({colVlr}) AS MAX_VLR
                FROM {schema}.{table} GROUP BY {aggCol}
                ) B ON MAX_VLR = {colVlr} AND A.{aggCol} = B.{aggCol}
        )
        """
        finalQuery_L.append(baseQuery)
    dicCidQueryBase = "\n\nUNION\n\n".join(finalQuery_L)
    dicCidQuery = f"""
        SELECT DISTINCT
            A.{aggCol},
            A.CID
        FROM ({dicCidQueryBase}) A
        INNER JOIN 
        (
            SELECT 
                {aggCol},
                MIN(PRIORIDADE) AS MAX_VLR
            FROM ({dicCidQueryBase}) GROUP BY {aggCol}
            ) B ON A.{aggCol} = b.{aggCol} AND A.PRIORIDADE = B.MAX_VLR
    """
    # print("Salvando dicionário de CIDs...")
    Utils.createSqlTableFromQuery(dicCidQuery, schema, "DIC_CIDS_TOPICO_7", session)
    
    # print("Fazendo merge com dicionario de CIDs e salvando...")
    queryFinal = f"""
    SELECT 
        A.*,
        B.CID
    FROM {schema}.{table} A
    LEFT JOIN DIC_CIDS_TOPICO_7 B
    ON A.{aggCol} = B.{aggCol}
"""
    Utils.createSqlTableFromQuery(queryFinal, schema, outputTable, session)
    # print("Fazendo drop da tabela dicionário de cids...")
    # session.sql("""DROP VIEW DIC_CIDS_TOPICO_7""").collect()