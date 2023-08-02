# -*- coding: utf-8 -*-
"""
Agrega internações com 1 dia ou menos de distância ou sobreposição de datas
"""

from snowflake.snowpark.functions import avg
import pandas as pd
import numpy as np
import json
import sys

## This is used to create a snowpark session
from snowflake.snowpark import Session

## Use this to create the UDF
from snowflake.snowpark.functions import udf, col, lag, lit
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.functions import dateadd as date_add
from snowflake.snowpark.types import StringType, IntegerType
from snowflake.snowpark.types import StructField as struct
import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

## These are necessary for the UDF function
import unidecode
import sys
import re
import os

# from Decorators import TimeExecution


# @TimeExecution
def aggregateHospitalStays( session:Session,
                            table:str,
                            schema=str,
                            outputTable="InternacoesAgregadas",
                            cod_cliente_colname="COD_CLIENTE",
                            initial_date_colname="DT_INTERNACAO",
                            final_date_colname="DT_ALTA",
                            dt_evento="DT_OCORR_EVENTO"
                         ):
    """
        sessionDataFilePath="sessionData.json"
        database="DEV"
        schema="RAW_SEGUROS"
        warehouse="RAW_PROCCESS"
        table="DtNascCorrigidas"
        role="RL_DEV"
        outputTable="InternacoesAgregadas"
        cod_cliente_colname="ID_PESSOA"
        initial_date_colname="DT_INTERNACAO"
        final_date_colname="DT_ALTA"
        dt_evento="DT_OCORR_EVENTO"
    """
    
    df = session.table(f"{schema}.{table}")
   
    ## Cria coluna TIPO_ATENDIMENTO (Usada para filtrar internações)
    df = df.withColumn("TIPO_ATENDIMENTO",
                       F.when((col("NUM_AGR_INTER") == " ") & 
                              (col("REGIME_DOMICILIAR") == 1),
                              "REGIME DOMICILIAR")
                       .when(col("NUM_AGR_INTER") != " ","INTERNACAO")
                       .otherwise("AMBULATORIO")
                       )

    ## Padroniza valores faltantes para None
    ## PASSO 1 : Preencher com DT_ATENDIMENTO quando vazio
    df =  df.withColumn("DT_INTERNACAO_CORRIGIDA",
        F.when(col(initial_date_colname) == ".", col(dt_evento)
                         ).otherwise(col(initial_date_colname)))
    df =  df.withColumn("DT_ALTA_CORRIGIDA",
        F.when(col(final_date_colname) == ".", col(dt_evento)
                         ).otherwise(col(final_date_colname)))
    
    ## PASSO 2 : Preencher com alta/entrada quando a contraparte estiver vazia
    df = df.withColumn("DT_INTERNACAO_CORRIGIDA",
        F.when((col("DT_INTERNACAO_CORRIGIDA") == ".") & (col("DT_ALTA_CORRIGIDA") != "."), col("DT_ALTA_CORRIGIDA"))
        .otherwise(col("DT_INTERNACAO_CORRIGIDA"))
                        )
    df = df.withColumn("DT_ALTA_CORRIGIDA",
        F.when((col("DT_ALTA_CORRIGIDA") == ".") & (col("DT_INTERNACAO_CORRIGIDA") != "."), col("DT_INTERNACAO_CORRIGIDA"))
        .otherwise(col("DT_ALTA_CORRIGIDA"))
                        )
    
    ## CAST TO DATE
    df = df.withColumn("DT_INTERNACAO_CORRIGIDA", F.to_date(col("DT_INTERNACAO_CORRIGIDA"), fmt="dd/mm/yyyy"))
    df = df.withColumn("DT_ALTA_CORRIGIDA", F.to_date(col("DT_ALTA_CORRIGIDA"), fmt="dd/mm/yyyy"))
    
    df = df.withColumn("DT_ALTA_CORRIGIDA",
        F.when(col("DT_INTERNACAO_CORRIGIDA") > col("DT_ALTA_CORRIGIDA"), col("DT_INTERNACAO_CORRIGIDA"))
        .otherwise(col("DT_ALTA_CORRIGIDA")))
    
    #df =  df.withColumn("DT_INTERNACAO_CORRIGIDA_2", F.least("DT_INTERNACAO_CORRIGIDA", "DT_ALTA_CORRIGIDA"))
    #df =  df.withColumn("DT_ALTA_CORRIGIDA_2",F.greatest("DT_INTERNACAO_CORRIGIDA", "DT_ALTA_CORRIGIDA"))

    ## filtrar apenas internacoes
    df = df.filter(col("TIPO_ATENDIMENTO") == "INTERNACAO")
    
    ## df.createOrReplaceTempView("TEMP")
    ## df.select("TIPO_ATENDIMENTO").take(100)
    window = Window.partitionBy(cod_cliente_colname).orderBy(
                col(cod_cliente_colname).asc(),
                col("DT_INTERNACAO_CORRIGIDA").asc(),
                col("DT_ALTA_CORRIGIDA").desc()
                ).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.orderBy(
            col(cod_cliente_colname).asc(),
            col("DT_INTERNACAO_CORRIGIDA").asc(),
            col("DT_ALTA_CORRIGIDA").desc()
        ).withColumn("DT_ALTA_CMAX", F.max("DT_ALTA_CORRIGIDA").over(window))
    
    
    ## a = pd.DataFrame(df.filter(df.DT_ALTA != ".").select("COD_CLIENTE", "DT_INTERNACAO","DT_ALTA","DT_ALTA_CMAX").take(1000));a = a.drop_duplicates(); a
    lagWindow = Window().partitionBy().orderBy(
            col(cod_cliente_colname).asc(),
            col("DT_INTERNACAO_CORRIGIDA").asc(),
            col("DT_ALTA_CORRIGIDA").desc()
        )
    
    print("Building lag columns...")
    df = df.withColumn("DT_INTERNACAO_CORRIGIDA_LAG", lag(col("DT_INTERNACAO_CORRIGIDA")).over(lagWindow))
    df = df.withColumn("DT_ALTA_CORRIGIDA_LAG", lag(col("DT_ALTA_CORRIGIDA")).over(lagWindow))
    df = df.withColumn("COD_CLIENTE_LAG", lag(col(cod_cliente_colname)).over(lagWindow))
    ## df.select("DT_INTERNACAO_CORRIGIDA_LAG").take(10)
    print("Building hospital stay IDs...")
    df = df.orderBy(
            col(cod_cliente_colname).asc(),
            col("COD_CLIENTE_LAG").asc(),
            col("DT_INTERNACAO_CORRIGIDA_LAG").asc(),
            col("DT_ALTA_CORRIGIDA_LAG").desc()
        ).withColumn("ID_INTERNACAO_PRE",
                     F.when(
            (col("COD_CLIENTE_LAG") == col(cod_cliente_colname))
        &
            ((date_add("day", lit(-1), col("DT_INTERNACAO_CORRIGIDA")) <= col("DT_INTERNACAO_CORRIGIDA_LAG")) &
            (date_add("day", lit(1), col("DT_ALTA_CMAX")) >= col("DT_INTERNACAO_CORRIGIDA_LAG")) 
        |
            (date_add("day", lit(-1), col("DT_INTERNACAO_CORRIGIDA")) <= col("DT_ALTA_CORRIGIDA_LAG")) &
            (date_add("day", lit(1), col("DT_ALTA_CMAX")) >= col("DT_ALTA_CORRIGIDA_LAG"))
        |
            ((col("DT_INTERNACAO_CORRIGIDA_LAG") == None) | (col("DT_ALTA_CORRIGIDA_LAG") == None)))
            , 0).otherwise(1)
                    )
                     
    df = df.withColumn("ID_INTERNACAO",
        F.sum(col("ID_INTERNACAO_PRE")).over(Window.partitionBy().orderBy(
            col(cod_cliente_colname).asc(),
            col("COD_CLIENTE_LAG").asc(),
            col("DT_INTERNACAO_CORRIGIDA").asc(),
            col("DT_INTERNACAO_CORRIGIDA_LAG").asc(),
            col("DT_ALTA_CORRIGIDA").desc(),
            col("DT_ALTA_CORRIGIDA_LAG").desc()
            ).rowsBetween(-sys.maxsize, 0)))

    df = df.drop([
        "COD_CLIENTE_LAG",
        "DT_ALTA_CORRIGIDA_LAG",
        "DT_INTERNACAO_CORRIGIDA_LAG",
        "DT_ALTA_CMAX",
        "DT_INTERNACAO",
        "DT_ALTA",
        "ID_INTERNACAO_PRE"
        ]).withColumnRenamed("DT_INTERNACAO_CORRIGIDA", "DT_INTERNACAO"
                             ).withColumnRenamed("DT_ALTA_CORRIGIDA", "DT_ALTA"
                                                 ).drop([
                                                     ""
                                                     ])

    df.write.mode('overwrite').saveAsTable(f"{schema}.{outputTable}")