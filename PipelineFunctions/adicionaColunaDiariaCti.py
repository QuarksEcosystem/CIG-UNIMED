# -*- coding: utf-8 -*-
"""
Conta quantidade de linhas na base.
"""
## This is used to create a snowpark session
from snowflake.snowpark import Session
# from Decorators import TimeExecution
import pandas as pd
import Utils
import json


# @TimeExecution
def adicionaColunaDiariaCti(session:Session,
                    database:str,
                    schema:str,
                    table:str,
                    outputTable="DiariasCti",
                    parameterFile="./diaria_cti.json"):
    """
        Conta a quantidade de linhas na base alvo.
    """

    ## select the database
    session.sql(f"""USE DATABASE {database}""").collect()
    session.sql(f"""USE SCHEMA {schema};""").collect()
    
    with open(parameterFile,"r", encoding="utf8") as f:
        params = json.loads(f.read())
        print(params['cod_acomodacao_tiss'])

    query = f"""
    SELECT
        A.*,
        CASE 
            WHEN COD_ACOMODACAO_TISS IN ({params['cod_acomodacao_tiss']}) THEN 'SIM'
            WHEN COD_ACOMODACAO_TISS_PEDIDO IN ({params['cod_acomodacao_tiss_pedido']}) THEN 'SIM'
            WHEN NOME_ACOMODACAO_PEDIDO IN ({params['nome_acomodacao_pedido']}) THEN 'SIM'
            WHEN NOME_ACOMODACAO IN ({params['nome_acomodacao']}) THEN 'SIM'
            ELSE 'NAO'
        END AS CTI_DIARIA
    FROM {schema}.{table} A
    WHERE
        COD_ACOMODACAO_TISS <> '6890427A1F51A3E7'
        AND COD_ACOMODACAO_TISS_PEDIDO <> '6890427A1F51A3E7'
    """
    Utils.createSqlTableFromQuery(query, schema, outputTable, session)

    # count = pd.DataFrame(session.sql(f"""SELECT 
    #                                      COUNT(*) AS COUNT,
    #                                      CTI_DIARIA
    #                                      FROM {schema}.{outputTable}
    #                                      GROUP BY CTI_DIARIA
    #                                      """).collect())
    # print(count)