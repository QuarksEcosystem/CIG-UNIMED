## Pipeline modules
from PipelineFunctions.consolidaTipoDeAlta_e_Internacao import consolidaTipoDeAlta_e_Internacao
from PipelineFunctions.trataCaracteresEspeciais import trataCaracteresEspeciais
from PipelineFunctions.consolidaDatasDeNascimento import consolidaDatasDeNascimento
from PipelineFunctions.trataCaracteresEspeciaisV2 import trataCaracteresEspeciais1
from PipelineFunctions.adicionaColunaDiariaCti import adicionaColunaDiariaCti
from PipelineFunctions.traduzirNomesDasColunas import traduzirNomesDasColunas
from PipelineFunctions.consolidaDadosPorValor import consolidaDadosPorValor
from PipelineFunctions.consolidaReinternacoes import consolidaReinternacoes
from PipelineFunctions.PadronizaCodigosCBHPM import PadronizaCodigosCBHPM
from PipelineFunctions.consolidaInternacoes import consolidaInternacoes
from PipelineFunctions.gerarTipoAtendimento import gerarTipoAtendimento
from PipelineFunctions.consolidaPostulantes import consolidaPostulantes
from PipelineFunctions.removeLinhas import removeLinhasComValores
from PipelineFunctions.geraCodControle2 import geraCodControle2
from PipelineFunctions.trataCids import trataCids
from PipelineFunctions.ContarLinhas import ContarLinhas
from PipelineFunctions.geraIdPessoa import geraIdPessoa
from connection.snowflakeconnection import connection, uploadToSnowflake,verif_insert_table,consulta_snow
from snowflake.snowpark import Session
from validacoes_pd import validation_rules_DataFrame, checa_completude
# from singletons.CiG_run import CiG_run
from pathlib import Path
import pandas as pd
import Utils
import re
import os


## RUN RHE PIPELINE
## Parameters
print("Setting parameters")
## TO-DO : In the future, replace these with command line arguments. 
database="CIG_DB"
schema="raw_input_clientes"
warehouse="streamlit_wh"
#table="BASE_SEGUROS"
## Quando o nome da base for 'case sensitive', coloque o nome entre ""(aspas duplas).
table='"SEGUROS1ANOCOMPLETO_RAW"'
role="streamlit_role"
# outputTable="tabCiGSnowflake"
tabSizes = []

def tasks_snow(session:Session):
    
    trataCaracteresEspeciais(session=session,
                             database=database,
                             schema=schema,
                             warehouse=warehouse,
                             table=table,
                             role=role,
                             outputTable="SemCaracteresEspeciais",
                             removedCharacters=[";",",","#","\*"])
    
    trataCaracteresEspeciais1(session=session,
        database=database,
        schema=schema,
        table='SemCaracteresEspeciais',
        outputTable="SEM_CARACT_ESPECIAIS",
        columns=['DS_PROD_MEDICA', 'TIPO_TABELA'])
    print("Tratamento 01 concluido")

    gerarTipoAtendimento(session=session,
        database=database,
        schema=schema,
        table="SEM_CARACT_ESPECIAIS",
        outputTable="TIPO_ATEND")
    print("Tratamento 02 concluido")


    geraIdPessoa(session=session,
             table="TIPO_ATEND",
             schema=schema,
             outputTable="COM_ID_PESSOA",
             colunaAlvo="COD_ENTIDADE_TS_SEGURADO",
             hierarquia=[
                "COD_ENTIDADE_TS_SEGURADO",
                "NUM_AGR_INTER",
                "NUM_PEDIDO_INTER",
                "COD_CONTROLE"])
    print("Tratamento 03 concluido")

    consolidaDatasDeNascimento(session=session,
                           schema=schema,
                           table="COM_ID_PESSOA",
                           outputTable="DT_NASC_CONSOLID")
    print("Tratamento 04 concluido")


    # # """erro"""
    consolidaInternacoes(
        session=session,
        schema=schema,
        table="DT_NASC_CONSOLID",
        outputTable="INTERNACOES_AGG",
        cod_cliente_colname="ID_PESSOA")
    # # """"""
    print('Tratamento 05 concluido ')

    geraCodControle2(session=session,
                 table="INTERNACOES_AGG",
                 schema=schema,
                 outputTable="COD_CONTROLE_2",
                 dt_referencia="DT_OCORR_EVENTO")
    print('Tratamento 06 concluido ')
    
    consolidaDadosPorValor(session=session,
                         database=database,
                         schema=schema,
                         table="COD_CONTROLE_2",
                         outputTable="INFO_CLIENTE",
                         aggregateColumn="SEXO_CLIENTE",
                         aggregationUnit="ID_PESSOA",
                         valueColumn="VLR_PROD_MEDICA",
                         aggType="MAX",
                         replaceOriginalColname=True)
    print('Tratamento 07 concluido ')
    
    consolidaDadosPorValor(session=session, 
                         database=database,
                         schema=schema,
                         table="INFO_CLIENTE",
                         outputTable="INFO_REDE",
                         aggregateColumn="TP_REDE",
                         aggregationUnit="ID_INTERNACAO",
                         valueColumn="VLR_PROD_MEDICA",
                         aggType="MAX",
                         replaceOriginalColname=True)
    print('Tratamento 08 concluido ')
    
    consolidaDadosPorValor(session=session,
                         database=database,
                         schema=schema,
                         table="INFO_REDE",
                         outputTable="ACOMODACAO_E_REGIME",
                         aggregateColumn="TP_ACOMODACAO",
                         aggregationUnit="ID_INTERNACAO",
                         valueColumn="VLR_PROD_MEDICA",
                         aggType="MAX",
                         replaceOriginalColname=True)
    print('Tratamento 09 concluido ')
    
    adicionaColunaDiariaCti(session=session,
                    database=database,
                    schema=schema,
                    table="ACOMODACAO_E_REGIME",
                    outputTable="DIARIA_CTI",
                    parameterFile="./parameters/seguros/diaria_cti.json")
    print('Tratamento 10 concluido ')
    
    consolidaDadosPorValor( session=session,
                         database=database,
                         schema=schema,
                         table="DIARIA_CTI",
                         outputTable="REGIME",
                         aggregateColumn="TIPO_REGIME",
                         aggregationUnit="ID_INTERNACAO",
                         valueColumn="VLR_PROD_MEDICA",
                         aggType="MAX",
                         replaceOriginalColname=True)
    print('Tratamento 11 concluido ')
    
    consolidaTipoDeAlta_e_Internacao(session=session,
                                 database=database,
                                 schema=schema,
                                 table="REGIME",
                                 outputTable="TIPO_ALTA_E_INTERNACAO")
    print('Tratamento 12 concluido ')
    
    
    trataCids(session=session,
        database=database,
        schema=schema,
        table="TIPO_ALTA_E_INTERNACAO",
        outputTable="INFO_CIDS",
        colVlr="VLR_PROD_MEDICA",
        colunasCid=["CID_SAIDA",
                    "CID_PRINC_ENTRADA",
                    "CID_SEC_ENTRADA"])
    print('Tratamento 13 concluido ')
    
    consolidaDadosPorValor(session=session,
                         database=database,
                         schema=schema,
                         table="INFO_CIDS",
                         outputTable="UF_INTERNACAO",
                         aggregateColumn="UF_EXECUTANTE",
                         aggregationUnit="ID_INTERNACAO",
                         valueColumn="VLR_PROD_MEDICA",
                         aggType="MAX",
                         replaceOriginalColname=True)
    print('Tratamento 14 concluido ')
    
    
    consolidaReinternacoes(session=session,
                         database=database,
                         schema=schema,
                         table="UF_INTERNACAO",
                         outputTable="DADOS_REINTERNACAO")
    print('Tratamento 15 concluido ')
    root=Path("./tabelas")
    
    
    for file_name in os.listdir(root):
        df = pd.read_stata( root / file_name)
        tabName = re.sub("[^A-Za-z0-9]", "_", file_name.split(".")[0]).upper()
        uploadToSnowflake(df=df,
                    session = session,
                    outputTableName = tabName,
                    database = database,
                    schema = schema,
                    temporary=True)
    
    
    PadronizaCodigosCBHPM(session=session,               
                        database=database,
                        schema=schema,
                        tabInternacoes="DADOS_REINTERNACAO",
                        outputTable="PADR_CBHPM",
                        schemaTabApoio=schema,
                        fontesTabApoio=['AMB90',
                                        'AMB92',
                                        'AMB96',
                                        'AMB99',
                                        'CBHPM_3',
                                        'CBHPM_4',
                                        'CBHPM_5'])
    print('Tratamento 16 concluido ')
    
    
    consolidaPostulantes(session=session,
                         database=database,
                         schema=schema,
                         table="PADR_CBHPM",
                         outputTable="Postulantes")
    print('Tratamento 17 concluido ')
    
    
    traduzirNomesDasColunas( session=session,
                         database=database,
                         schema=schema,
                         table="Postulantes",
                         outputTable="tabCiGSnowflake",
                         arquivoDeTraducao="./traducao/traducaoSeguros.json")
    print('Tratamento 18 concluido ')
    
    
    
# import json
# keys = json.loads(open("./keys/key.json").read())
# key = keys["admin"]
# session = connection(key)
# df = pd.read_csv('./dados_tests/ExemploUnio.csv', sep=';', dtype={'COD_PREST': str}, nrows=100)

# df["CID_SAIDA"] = df["CID_SAIDA"].astype(str, errors='ignore')
# df["CID_PRINC_ENTRADA"] = df["CID_PRINC_ENTRADA"].astype(str, errors='ignore')
# df["CID_SEC_ENTRADA"] = df["CID_SEC_ENTRADA"].astype(str, errors='ignore')
# df["UF_EXECUTANTE"] = df["UF_EXECUTANTE"].astype(str, errors='ignore')
# df["UF_SOLICITANTE"] = df["UF_SOLICITANTE"].astype(str, errors='ignore')
# df["COD_ITEM_PROD_MEDICA"] = df["COD_ITEM_PROD_MEDICA"].astype(int, errors='ignore')
# df["QTDE_PROD_MEDICA"] = df["QTDE_PROD_MEDICA"].astype(int, errors='ignore')
# df["CRM_EXECUTANTE"] = df["CRM_EXECUTANTE"].astype(int, errors='ignore')
# df["CRM_SOLICITANTE"] = df["CRM_SOLICITANTE"].astype(int, errors='ignore')
# df["VLR_PROD_MEDICA"] = df["VLR_PROD_MEDICA"].astype(float, errors='ignore')
# #tenta setar as colunas com o datatype correto pois o dataframe pode atribuir datatype object para as colunas

# checa_completude(df)
# print(df)
# print(validation_rules_DataFrame(df))

#uploadToSnowflake(df=df,
#               session = session,
#                outputTableName = 'SEGUROS1ANOCOMPLETO_RAW',
#               database = 'CIG_DB',
#               schema = 'raw_input_clientes',
#               temporary=True)
#print(df.head(10))
#tasks_snow(session=session)
#query = f"""SELECT * FROM CIG_DB.raw_input_clientes.tabCiGSnowflake"""
#value = pd.DataFrame(session.sql(query).collect())
#print(value.head(10))
