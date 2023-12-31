# -*- coding: utf-8 -*-
"""
Replace special charecters in a snowflake table using a Snowpark UDF.
"""

from snowflake.snowpark.functions import avg
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


def replaceSpecialCharacters(session:Session,
                             database:str,
                             schema:str,
                             warehouse:str,
                             table:str,
                             role:str,
                             outputTable="SemCaracteresEspeciais"):

#     print("Selecting database, warehouse and schema...")
    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    session.sql(f"""USE WAREHOUSE {warehouse};""").collect()

#     print("Updating function permissions...")
    session.sql(f"""grant USAGE, CREATE FUNCTION on schema {schema} to {role};""").collect()

    print("Adding required packages ...")
    session.add_packages("unidecode")

    specialCharReplace = session.udf.register(
                             lambda txt: unidecode.unidecode(txt).upper(),
                             packages=["unidecode"],
                             return_type=StringType(),
                             input_types=[StringType()],
                             name="specialCharReplace",
                             replace=True,
                             stage_location="@~"
                             )

    session.sql(f"""
    CREATE OR REPLACE TABLE {outputTable} AS
    (SELECT 
         SPECIALCHARREPLACE(COD_CONTROLE) AS COD_CONTROLE,
         SPECIALCHARREPLACE(COD_PREST) AS COD_PREST,
         SPECIALCHARREPLACE(TP_REDE) AS TP_REDE,
         SPECIALCHARREPLACE(COD_UNIMED) AS COD_UNIMED,
         SPECIALCHARREPLACE(COD_CLIENTE) AS COD_CLIENTE,
         SPECIALCHARREPLACE(DT_NASCIMENTO_CLIENTE) AS DT_NASCIMENTO_CLIENTE,
         SPECIALCHARREPLACE(SEXO_CLIENTE) AS SEXO_CLIENTE,
         SPECIALCHARREPLACE(TP_ACOMODACAO) AS TP_ACOMODACAO,
         SPECIALCHARREPLACE(TP_PRODUTO) AS TP_PRODUTO,
         SPECIALCHARREPLACE(TP_CONTRATACAO) AS TP_CONTRATACAO,
         SPECIALCHARREPLACE(DT_INICIO_INTERNACAO) AS DT_INICIO_INTERNACAO,
         SPECIALCHARREPLACE(DT_FIM_INTERNACAO) AS DT_FIM_INTERNACAO,
         SPECIALCHARREPLACE(NUM_AGR_INTER) AS NUM_AGR_INTER,
         SPECIALCHARREPLACE(TIPO_ALTA) AS TIPO_ALTA,
         SPECIALCHARREPLACE(TIPO_INTERNACAO_CONTA) AS TIPO_INTERNACAO_CONTA,
         SPECIALCHARREPLACE(TIPO_REGIME) AS TIPO_REGIME,
         SPECIALCHARREPLACE(CID_PRINC_ENTRADA) AS CID_PRINC_ENTRADA,
         SPECIALCHARREPLACE(CID_SEC_ENTRADA) AS CID_SEC_ENTRADA,
         SPECIALCHARREPLACE(CID_SAIDA) AS CID_SAIDA,
         SPECIALCHARREPLACE(NUM_PEDIDO_INTER) AS NUM_PEDIDO_INTER,
         SPECIALCHARREPLACE(TIPO_INTERNACAO) AS TIPO_INTERNACAO,
         SPECIALCHARREPLACE(TIPO_INTERNACAO_PEDIDO) AS TIPO_INTERNACAO_PEDIDO,
         SPECIALCHARREPLACE(NUM_SENHA_INTER) AS NUM_SENHA_INTER,
         SPECIALCHARREPLACE(DT_OCORR_EVENTO) AS DT_OCORR_EVENTO,
         SPECIALCHARREPLACE(COD_ITEM_PROD_MEDICA) AS COD_ITEM_PROD_MEDICA,
         SPECIALCHARREPLACE(DS_PROD_MEDICA) AS DS_PROD_MEDICA,
         SPECIALCHARREPLACE(TIPO_TABELA) AS TIPO_TABELA,
         SPECIALCHARREPLACE(TIPO_PROD_MEDICA) AS TIPO_PROD_MEDICA,
         SPECIALCHARREPLACE(COD_PROCEDIMENTO_CM) AS COD_PROCEDIMENTO_CM,
         SPECIALCHARREPLACE(QTDE_PROD_MEDICA) AS QTDE_PROD_MEDICA,
         SPECIALCHARREPLACE(VLR_PROD_MEDICA) AS VLR_PROD_MEDICA,
         SPECIALCHARREPLACE(CONSELHO_SOLICITANTE) AS CONSELHO_SOLICITANTE,
         SPECIALCHARREPLACE(CRM_SOLICITANTE) AS CRM_SOLICITANTE,
         SPECIALCHARREPLACE(UF_SOLICITANTE) AS UF_SOLICITANTE,
         SPECIALCHARREPLACE(CONSELHO_EXECUTANTE) AS CONSELHO_EXECUTANTE,
         SPECIALCHARREPLACE(UF_EXECUTANTE) AS UF_EXECUTANTE,
         SPECIALCHARREPLACE(NOME_PROCEDIMENTO_PRINCIPAL) AS NOME_PROCEDIMENTO_PRINCIPAL,
         SPECIALCHARREPLACE(CARATER_CONTA_MEDICA) AS CARATER_CONTA_MEDICA,
         SPECIALCHARREPLACE(NOME_EXEC_AUTORIZACAO) AS NOME_EXEC_AUTORIZACAO,
         SPECIALCHARREPLACE(NOME_ACOMODACAO) AS NOME_ACOMODACAO,
         SPECIALCHARREPLACE(NOME_ACOMODACAO_PEDIDO) AS NOME_ACOMODACAO_PEDIDO,
         SPECIALCHARREPLACE(NOME_TRATAMENTO_PEDIDO) AS NOME_TRATAMENTO_PEDIDO,
         SPECIALCHARREPLACE(TIPO_REGIME_PEDIDO) AS TIPO_REGIME_PEDIDO,
         SPECIALCHARREPLACE(DS_PRESTADOR_ORIGEM) AS DS_PRESTADOR_ORIGEM,
         SPECIALCHARREPLACE(DS_PRESTADOR_CABECA) AS DS_PRESTADOR_CABECA,
         SPECIALCHARREPLACE(DS_PRESTADOR) AS DS_PRESTADOR,
         SPECIALCHARREPLACE(CIDADE_PRESTADOR) AS CIDADE_PRESTADOR,
         SPECIALCHARREPLACE(UF_PRESTADOR) AS UF_PRESTADOR,
         SPECIALCHARREPLACE(NOME_VINCULACAO) AS NOME_VINCULACAO,
         SPECIALCHARREPLACE(NOME_TIPO_PRESTADOR) AS NOME_TIPO_PRESTADOR,
         SPECIALCHARREPLACE(GRUPO_PRESTADOR) AS GRUPO_PRESTADOR,
         SPECIALCHARREPLACE(GP_EXECUTANTE) AS GP_EXECUTANTE,
         SPECIALCHARREPLACE(DT_INTERNACAO) AS DT_INTERNACAO,
         SPECIALCHARREPLACE(DT_ALTA) AS DT_ALTA,
         SPECIALCHARREPLACE(CBO_SOLICITANTE) AS CBO_SOLICITANTE,
         SPECIALCHARREPLACE(CRM_EXECUTANTE) AS CRM_EXECUTANTE,
         SPECIALCHARREPLACE(CBO_EXECUTANTE) AS CBO_EXECUTANTE,
         SPECIALCHARREPLACE(COD_PROCEDIMENTO_PRINCIPAL) AS COD_PROCEDIMENTO_PRINCIPAL,
         SPECIALCHARREPLACE(COD_EXEC_AUTORIZACAO) AS COD_EXEC_AUTORIZACAO,
         SPECIALCHARREPLACE(CARATER_AUTORIZACAO_PEDIDO) AS CARATER_AUTORIZACAO_PEDIDO,
         SPECIALCHARREPLACE(COD_ACOMODACAO_TISS) AS COD_ACOMODACAO_TISS,
         SPECIALCHARREPLACE(COD_ACOMODACAO_TISS_PEDIDO) AS COD_ACOMODACAO_TISS_PEDIDO,
         SPECIALCHARREPLACE(COD_ENTIDADE_TS_SEGURADO) AS COD_ENTIDADE_TS_SEGURADO,
         SPECIALCHARREPLACE(COD_PRESTADOR_ORIGEM) AS COD_PRESTADOR_ORIGEM,
         SPECIALCHARREPLACE(NUM_INSC_FISCAL_ORIGEM) AS NUM_INSC_FISCAL_ORIGEM,
         SPECIALCHARREPLACE(TIPO_PRESTADOR_ORIGEM) AS TIPO_PRESTADOR_ORIGEM,
         SPECIALCHARREPLACE(COD_PRESTADOR_CABECA) AS COD_PRESTADOR_CABECA,
         SPECIALCHARREPLACE(NUM_INSC_FISCAL_CABECA) AS NUM_INSC_FISCAL_CABECA,
         SPECIALCHARREPLACE(TIPO_PRESTADOR_CABECA) AS TIPO_PRESTADOR_CABECA,
         SPECIALCHARREPLACE(ID_GRUPO) AS ID_GRUPO,
         SPECIALCHARREPLACE(CODIGO_ESTIPULANTE) AS CODIGO_ESTIPULANTE,
         SPECIALCHARREPLACE(NR_CONTRATO) AS NR_CONTRATO,
         SPECIALCHARREPLACE(REGIME_DOMICILIAR) AS REGIME_DOMICILIAR,
         SPECIALCHARREPLACE(NIVEL_1_GRUPO_TP_DESPESA) AS NIVEL_1_GRUPO_TP_DESPESA,
         SPECIALCHARREPLACE(COD_PRESTADOR_TS) AS COD_PRESTADOR_TS,
         SPECIALCHARREPLACE(COD_ENT_TS_PREST) AS COD_ENT_TS_PREST,
         SPECIALCHARREPLACE(COD_PRESTADOR) AS COD_PRESTADOR,
         SPECIALCHARREPLACE(NR_CNPJ) AS NR_CNPJ
    FROM {schema}.{table});
                """).collect()
    #LIMIT 10
#     session.close()