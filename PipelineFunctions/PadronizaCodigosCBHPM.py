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
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import udf, col
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.types import StringType

## These are necessary for the UDF function
from pathlib import Path
import networkx as nx
from tqdm import tqdm
import unidecode
import sys
import re
import os

# from login.SnowflakeSession import SnowflakeSession
import Utils

def uploadArquivosStataParaSnowflake(root : Path,
                                    session : Session,
                                    database:str,
                                    schema:str):
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""CREATE SCHEMA IF NOT EXISTS {schema}""").collect()
    sources = []
    for f in tqdm(os.listdir(root)):
        df = pd.read_stata(root/f)
        df_c = Utils.castInvalidFormats(df)
        tabName = re.sub("[^A-Za-z0-9]","_", f.split(".")[0]).upper()
        sources.append(tabName)
        # print("Uploading",tabName,"...")
        Utils.uploadToSnowflake(df=df_c, session=session, outputTableName = tabName, database=database, schema=schema,temporary=True)
        # print(tabName)
    # print("Upload completo, tabelas:",sources


def criaTabelaDeApoio(
        session : Session,
        schema:str,
        database:str,
        outputTable="TabelaDeApoio",
        fontes=['AMB90',
                'AMB92',
                'AMB96',
                'AMB99',
                'CBHPM_3',
                'CBHPM_4',
                'CBHPM_5'],
        additionalMapping=True):
    
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    
    with open("./sql/criarTabelaDeApoio.sql","r") as f:
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
    
    # print("Criando tabela de apoio...")
    result = Utils.createSqlTableFromQuery(tabelComPorteAnest, schema, "TAB_APOIO_COM_PORTE_ANEST", session)
    # print(result)
    tabApoio = session.table(f"{schema}.TAB_APOIO_COM_PORTE_ANEST")
    # print("Dropping duplicates...",tabApoio.count())
    tabApoioNoDuplic = tabApoio.dropDuplicates(['"cod_item_prod_medica_VIDAS"','"cdigotuss_90"'])
    # print("Remaining:",tabApoioNoDuplic.count())
    
    codigos = pd.DataFrame(tabApoioNoDuplic.collect())
    dicionarioDeCodigos = {}
    mapping = codigos.set_index("cod_item_prod_medica_VIDAS").cdigotuss_90.to_dict()
    for key in tqdm(mapping):
        finalMap = mapping[key]
        while finalMap in mapping and finalMap != mapping[finalMap]:
            finalMap = mapping[finalMap]
        dicionarioDeCodigos.update({key:finalMap})
    if additionalMapping:
        codigos["CodFinal"] = [dicionarioDeCodigos[x] for x in tqdm(codigos.cod_item_prod_medica_VIDAS)]
    else:
        codigos["CodFinal"] = codigos.cod_item_prod_medica_VIDAS.values
    tab = session.write_pandas(codigos, f"{schema}.TAB_APOIO_COM_PORTE_ANEST",
                         overwrite=True)
    # tab.write.mode("overwrite").saveAsTable(f"{schema}.TAB_APOIO_COM_PORTE_ANEST")
    tab.createOrReplaceView(f"{schema}.TAB_APOIO_COM_PORTE_ANEST")
    return f"{schema}.TAB_APOIO_COM_PORTE_ANEST"


           

# from Decorators import TimeExecution

# @TimeExecution
def PadronizaCodigosCBHPM(session:Session,
                         database:str,
                         schema:str,
                         tabInternacoes:str,
                         outputTable="CodigosPadronizados",
                         schemaTabApoio="TAB_APOIO",
                         codProdMedCol="COD_ITEM_PROD_MEDICA",
                         fontesTabApoio=['AMB90',
                                         'AMB92',
                                         'AMB96',
                                         'AMB99',
                                         'CBHPM_3',
                                         'CBHPM_4',
                                         'CBHPM_5']
                         ):
    """
    database="DEV"
    schema="RAW_SEGUROS"
    warehouse="RAW_PROCCESS"
    tabInternacoes='"Seguros1AnoCompleto_Raw"'
    outputTable="CodigosPadronizados"
    schemaTabApoio="TAB_APOIO"
    codProdMedCol="COD_ITEM_PROD_MEDICA"
    fontesTabApoio=['AMB90',
                    'AMB92',
                    'AMB96',
                    'AMB99',
                    'CBHPM_3',
                    'CBHPM_4',
                    'CBHPM_5']
    """

    # print("Selecting database, warehouse and schema...")
    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    
    ## STEP 1 
    ## Criar uma tabela de apoio.
    tabApoioNome = criaTabelaDeApoio(session,
                      schema=schemaTabApoio,
                      database=database,
                      outputTable="TabelaDeApoio",
                      fontes=fontesTabApoio)
    
    ## STEP 2
    ## merge da tabela de apoio para a base de internações.
    ##     CREATE OR REPLACE TEMPORARY TABLE TEMP_COD_INTERN AS
    queryTabInternacoesTemp = f"""
    CREATE OR REPLACE TEMPORARY TABLE TEMP_COD_PROD_MED_PADR AS
    (
     SELECT * FROM 
         (SELECT DISTINCT
            {codProdMedCol},
            DS_PROD_MEDICA,
            CASE 
                WHEN 'CodFinal' <> NULL THEN 'CodFinal'
                ELSE {codProdMedCol}
             END AS COD_PROD_MED_PADRONIZADO
        FROM {schema}.{tabInternacoes}
        LEFT JOIN 
        {tabApoioNome} 
        ON {codProdMedCol} = 'CodFinal')
    WHERE 
        DS_PROD_MEDICA <> '' AND  DS_PROD_MEDICA IS NOT NULL AND DS_PROD_MEDICA <> ' ' AND
        {codProdMedCol} <> '' AND  {codProdMedCol} IS NOT NULL AND {codProdMedCol} <> ' '
        AND  {codProdMedCol} <> '0'
    )
"""
    # print("Criando tabela temporária de códigos...")
    result = session.sql(queryTabInternacoesTemp).collect()
    # print(result)
    
    ## Seleciona os itens de descrição mais longas como 'descrição padronizada'
    queryDescricoes = f"""
        CREATE OR REPLACE TEMPORARY TABLE TAB_DESCRICOES_PADRONIZADAS AS
        (SELECT DISTINCT
            b.* 
        FROM
        (
            SELECT
            COD_PROD_MED_PADRONIZADO AS {codProdMedCol},
            MAX(LEN(DS_PROD_MEDICA)) AS TAMANHO
            FROM TEMP_COD_PROD_MED_PADR
            GROUP BY COD_PROD_MED_PADRONIZADO
        ) a
        INNER JOIN 
        (
            SELECT
                COD_PROD_MED_PADRONIZADO AS {codProdMedCol},
                DS_PROD_MEDICA,
                LEN(DS_PROD_MEDICA) AS TAMANHO
            FROM TEMP_COD_PROD_MED_PADR
        ) b ON a.{codProdMedCol} = b.{codProdMedCol})
    """
    # print("Criando tabela temporária de descrições...")
    result = session.sql(queryDescricoes).collect()
    # print(result)
    
    queryDescricoesUnicas = f"""
    CREATE OR REPLACE TEMPORARY TABLE TAB_DESCRICOES_PADRONIZADAS_UNICAS AS
    (
     SELECT A.* FROM TAB_DESCRICOES_PADRONIZADAS A
     INNER JOIN
         (
          SELECT 
              MAX(TAMANHO) AS TAMANHO,
              {codProdMedCol}
          FROM TAB_DESCRICOES_PADRONIZADAS
          GROUP BY {codProdMedCol}
          ) B
         ON A.{codProdMedCol} = B.{codProdMedCol} AND A.TAMANHO = B.TAMANHO
     )
"""
    # print("Filtrando códigos únicos...")
    result = session.sql(queryDescricoesUnicas).collect()
    # print(result)
    # session.sql("""SELECT COUNT(*) FROM TAB_DESCRICOES_PADRONIZADAS_UNICAS""").collect()
    
    ## TIPOS DE PACOTE
    tipos_de_pacote = {
        "TABELA PRÓPRIA DE PACOTES" : "P",
        "MEDICAMENTOS" : "M",
        "PROCEDIMENTOS E EVENTOS EM SAÚDE" : "I",
        "DIÁRIAS TAXAS E GASES MEDICINAIS" : "S",
        "DIÁRIAS, TAXAS E GASES MEDICINAIS" : "S",
        "TABELA PRÓPRIA DAS OPERADORAS" : "M",
        "PROCEDIMENTOS MÉDICOS" : "I",
        "MATERIAIS E ÓRTESES PRÓTESES E MATERIAIS ESPECIAIS (OPME)" : "M",
        "MATERIAIS E ÓRTESES, PRÓTESES E MATERIAIS ESPECIAIS (OPME)" : "M",
        "TABELA PRÓPRIA PACOTE ODONTOLÓGICO" : "P",
        "" : "M",
        }

    colTipoProdMed = "\n".join([f"""WHEN TIPO_TABELA = '{k}' AND TIPO_PROD_MEDICA = '' THEN '{tipos_de_pacote[k]}' """ for k in tipos_de_pacote])
    
    queryDescrProdMed = f"""
    SELECT 
        INTERNACOES.*,
        DESCRICOES.COD_ITEM_PROD_MEDICA AS COD_PRD_MED_TABELA,
        CASE 
            WHEN DESCRICOES.DS_PROD_MEDICA IS NOT NULL AND DESCRICOES.DS_PROD_MEDICA <> '' THEN DESCRICOES.DS_PROD_MEDICA
            ELSE INTERNACOES."DS_PROD_MEDICA"
        END AS DS_PROD_MEDICA_CiG,
        CASE 
            {colTipoProdMed}
            ELSE TIPO_TABELA
        END AS TIPO_PROD_MEDICA_CIG
    FROM {schema}.{tabInternacoes} INTERNACOES
    LEFT JOIN (SELECT * FROM TAB_DESCRICOES_PADRONIZADAS_UNICAS) DESCRICOES 
        ON INTERNACOES.{codProdMedCol} = DESCRICOES.{codProdMedCol}
"""
    # print("Criando tabela de descrições e códigos corrigidos...")
    Utils.createSqlTableFromQuery(queryDescrProdMed, schema, outputTable, session)
    
    ## Log changes
    # check = pd.DataFrame(session.sql(f"""
    # SELECT COUNT(TIPO_PROD_MEDICA_CIG) AS COUNT,
    # TIPO_PROD_MEDICA_CIG
    # FROM {schema}.{outputTable}
    # GROUP BY TIPO_PROD_MEDICA_CIG;""").collect())
    # print(check)
    
    # unique = pd.DataFrame(session.sql(f"""
    # SELECT COUNT(DISTINCT(DS_PROD_MEDICA_CiG)) AS UNIQUE_VALUES_COUNT
    # FROM {schema}.{outputTable}""").collect())
    # print("Número de descrições únicas:\n",unique.values[0])
    