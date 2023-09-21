import pandas as pd
import numpy as np
from datetime import datetime
import re
import ast
import inspect

#Valida se o dataframe contém a quantidade minima de campos mandatórios preenchidos
#def validate_completeness(df):

def validate_date(value):
    if pd.notnull(value) and re.match(r'\d{2}/\d{2}/\d{4}', value):
        return pd.to_datetime(value, format='%d/%m/%Y', errors='coerce').strftime('%d/%m/%Y')
    return ''


def validate_string(value):
    return isinstance(value, str)


def validate_cid(value):
    return re.match(r'^[A-Z]\d{2}$', value) is not None


def validate_integer(value):
    return isinstance(value, int)


def validate_float(value):
    return isinstance(value, float)


def validate_uf(value):
    return isinstance(value, str) and len(value) == 2


def validation_rules_DataFrame(df):
    validation_rules = {
        'COD_GUIA': validate_string,
        'DT_OCORR_EVENTO': validate_date,
        'DT_INICIO_GUIA_INTERNACAO': validate_date,
        'DT_FIM_GUIA_INTERNACAO': validate_date,
        'TIPO_ATENDIMENTO': validate_string,
        'REGIME_DO_ATENDIMENTO': validate_string,
        'CARATER_CONTA_MEDICA': validate_string,
        'TP_ACOMODACAO': validate_string,
        'TIPO_ALTA': validate_string,
        'TIPO_INTERNACAO_CONTA': validate_string,
        'TIPO_REGIME': validate_string,
        'CID_GUIA': validate_cid,
        'CID_SAIDA': validate_cid,
        'CID_PRINC_ENTRADA': validate_cid,
        'CID_SEC_ENTRADA': validate_cid,
        'COD_ITEM_PROD_MEDICA': validate_string,
        'DS_PROD_MEDICA': validate_string,
        'TIPO_PROD_MEDICA': validate_string,
        'TIPO_TABELA': validate_string,
        'QTDE_PROD_MEDICA': validate_string,
        'VLR_PROD_MEDICA': validate_float,
        'COD_PRESTADOR_EXECUTANTE_PJ': validate_string,
        'DS_PRESTADOR_EXECUTANTE_PJ': validate_string,
        'NOME_TIPO_PRESTADOR_PJ': validate_string,
        'CIDADE_PRESTADOR_PJ': validate_string,
        'UF_PRESTADOR_PJ': validate_uf,
        'COD_UNIMED_PRESTADOR_EXC_PJ': validate_string,
        'TP_REDE_PJ': validate_string,
        'GRUPO_PRESTADOR_PJ': validate_string,
        'COD_CLIENTE': validate_string,
        'DT_NASCIMENTO_CLIENTE': validate_date,
        'SEXO_CLIENTE': validate_string,
        'GENERO_CLIENTE': validate_string,
        'TP_PRODUTO': validate_string,
        'TP_CONTRATACAO': validate_string, #and value in ['EMPRESARIAL', 'ADESAO', 'INDIVIDUAL'],
        'CODIGO_ESTIPULANTE': validate_string,
        'ID_GRUPO_CLIENTE _EMPRESARIAL': validate_string,
        'CRM_EXECUTANTE': validate_string,
        'CONSELHO_EXECUTANTE': validate_string,
        'GP_EXECUTANTE': validate_string,# and value in ['CIRURGIÃO', 'ANESTESISTA', 'AUXILIAR DO CIRURGIÃO', 'AUXILIAR DO ANESTESISTA'],
        'UF_EXECUTANTE': validate_uf,
        'NOME_EXEC_AUTORIZACAO': validate_string,
        'CRM_SOLICITANTE': validate_string,
        'CONSELHO_SOLICITANTE': validate_string,
        'UF_SOLICITANTE': validate_uf,
        'CLIENTE': validate_string
    }

    # def apply_validation_rules(df, rules):
    #     is_valid = pd.Series(True, index=df.index)
    #     for column, rule in rules.items():
    #         if column in df.columns:
    #             values = df[column]
    #             is_valid = np.logical_and(is_valid, values.apply(rule))
    #     return df[is_valid] '%d/%m/%Y'
    
    expected_types = {
        'COD_GUIA': str,
        'DT_OCORR_EVENTO': '%d/%m/%Y',
        'DT_INICIO_GUIA_INTERNACAO': '%d/%m/%Y',
        'DT_FIM_GUIA_INTERNACAO': '%d/%m/%Y',
        'TIPO_ATENDIMENTO': str,
        'REGIME_DO_ATENDIMENTO': str,
        'CARATER_CONTA_MEDICA': str,
        'TP_ACOMODACAO': str,
        'TIPO_ALTA': str,
        'TIPO_INTERNACAO_CONTA': str,
        'TIPO_REGIME': str,
        'CID_GUIA': str,
        'CID_SAIDA': str,
        'CID_PRINC_ENTRADA': str,
        'CID_SEC_ENTRADA': str,
        'COD_ITEM_PROD_MEDICA': str,
        'DS_PROD_MEDICA': str,
        'TIPO_PROD_MEDICA': str,
        'TIPO_TABELA': str,
        'QTDE_PROD_MEDICA': str,
        'VLR_PROD_MEDICA': str,
        'COD_PRESTADOR_EXECUTANTE_PJ': str,
        'DS_PRESTADOR_EXECUTANTE_PJ': str,
        'NOME_TIPO_PRESTADOR_PJ': str,
        'CIDADE_PRESTADOR_PJ': str,
        'UF_PRESTADOR_PJ': str,
        'COD_UNIMED_PRESTADOR_EXC_PJ': str,
        'TP_REDE_PJ': str,
        'GRUPO_PRESTADOR_PJ': str,
        'COD_CLIENTE': str,
        'DT_NASCIMENTO_CLIENTE': '%d/%m/%Y',
        'SEXO_CLIENTE': str,
        'GENERO_CLIENTE': str,
        'TP_PRODUTO': str,
        'TP_CONTRATACAO': str,
        'CODIGO_ESTIPULANTE': str,
        'ID_GRUPO_CLIENTE _EMPRESARIAL': str,
        'CRM_EXECUTANTE': str,
        'CONSELHO_EXECUTANTE': str,
        'GP_EXECUTANTE': str,
        'UF_EXECUTANTE': str,
        'NOME_EXEC_AUTORIZACAO': str,
        'CRM_SOLICITANTE': str,
        'CONSELHO_SOLICITANTE': str,
        'UF_SOLICITANTE': str,
        'CLIENTE': str
    }


    def apply_validation_rules(df, rules, expected_types):
        invalid_columns = []  
        expected_values = {} 
        invalid_values = {}

        # Iterar sobre as regras de validação
        for column, rule in rules.items():
            if column in df.columns:  
                values = df[column]  
                is_valid = values.apply(rule)
                if not is_valid.all(): 
                    invalid_values[column] = values[~is_valid.apply(bool)] 
                    invalid_columns.append(column) 
                    expected_values[column] = expected_types.get(column) 

        if invalid_columns: 
            # Criar um DataFrame com as colunas inválidas e os tipos esperados correspondentes
            invalid_df = pd.DataFrame({'Column': invalid_columns, 
                                       'Expected Type': [expected_values.get(col) if expected_values.get(col) else 'Unknown' for col in invalid_columns]})
            print(invalid_values)
            return invalid_df
        else:
            return None  # Retornar o DataFrame original se não houver colunas inválidas


    df_validated = apply_validation_rules(df, validation_rules, expected_types)

    return df_validated



    
# Exemplo de DataFrame com os dados
data = {
    'codigoGuia': [524320920, 2],
    'dataOcorrenciaEvento': ['27/09/2019', '27/09/2019'],
    'dataInicioGuiaInternacao': ['08/07/2019', '08/07/2019'],
    'dataFimGuiaInternacao': ['17/02/2021', '17/02/2021'],
    'tpAtendimento': ['INTERNAÇÃO', 'test'],
    'dsRegimeAtendimento': ['INTERNAÇÃO', 'test'],
    'dsCaraterContaMedica': ['ELETIVA', 'test'],
    'tpAcomodacao': ['ENFERMARIA', 'test'],
    'tpAlta': ['ALTA ADMINISTRATIVA', 'test'],
    'tpInternacaoConta': ['CIRÚRGICA', 'test'],
    'tpRegime': ['HOSPITALAR', 'test'],
    'codigoCID': ['C16', 'tes'],
    'codigoCIDSaida': ['C16','tes'],
    'codigoCIDPrincipalEntrada': ['C18', 'tes'],
    'codigoCIDSecudarioEntrada': ['C18', 'tes'],
    'codigoItemProducaoMedica': [163877, 32424],
    'dsProducaoMedica': ['ELETRODO MONITORIZACAO ESPUMA 2223BRQ', 'test'],
    'tpProducaoMedica': ['HONORARIO', 'test'],
    'tpTabela': ['PRÓPRIA', 'test'],
    'quantidadeProducaoMedica': [10, 10],
    'valorProducaoMedica': [12.15, 10.1],
    'codigoPrestador': [1275, 312],
    'dsPrestador': ['HOSPITAL PORTUGUES', 'test'],
    'nomeTipoPrestador': ['HOSPITAL GERAL', 'test'],
    'cidadePrestador': ['SALVADOR', 'test'],
    'unidadefederativaPrestador': ['BA', 'test'],
    'codigoUnimedPrestadora': [102, 323],
    'tpRedePrestador': ['REDE DIRETA', 'test'],
    'grupoPrestador': ["REDE DOR", 'test'],
    'codigoBeneficiario': [2855941,412412],
    'dataNascimento': ['25/07/1990', '25/07/1990'],
    'tpSexo': ['M' , 'n'],
    'generoBeneficiario': ['', '25/07/1990'],
    'tpProduto': ['BASICO', 'test'],
    'tpContratacao': ['EMPRESARIAL', 'test'],
    'codigoEstipulante': [2262581, 23132],
    'idGrupoClienteEmpresarial': [1280, 12312],
    'numeroConselhoExecutante': [7144,4242],
    'siglaConselhoExecutante': ['CRM', 'test'],
    'grauParticipacaoExecutante': ['CIRURGIÃO', 'test'],
    'unidadefederativaExecutante': ['RJ', 'test'],
    'nomeMedicoExecutante': ['HOSPITAL PORTUGUES', 'test'],
    'numeroConselhoSolicitante': [204395,312312],
    'siglaConselhoSolicitante': ['CRM', 'test'],
    'unidadefederativaSolicitante': ['RJ', 'test']
}

def checa_completude(df):
    expected_completeness = {
        'Unnamed: 0': 1.0,
        'COD_CONTROLE': 0.8,
        'COD_PREST': 1.0,
        'TP_REDE': 0.0,
        'COD_UNIMED': 1.0,
        'COD_CLIENTE': 1.0,
        'DT_NASCIMENTO_CLIENTE': 0.8,
        'SEXO_CLIENTE': 0.8,
        'TP_ACOMODACAO': 0.8,
        'TP_PRODUTO': 0.0,
        'TP_CONTRATACAO': 0.0,
        'DT_INICIO_INTERNACAO': 0.8,
        'DT_FIM_INTERNACAO': 0.8,
        'NUM_AGR_INTER': 0.0,#?
        'TIPO_ALTA': 0.8,
        'TIPO_INTERNACAO_CONTA': 0.0,
        'TIPO_REGIME': 0.0,
        'CID_PRINC_ENTRADA': 0.8,
        'CID_SEC_ENTRADA': 0.0,
        'CID_SAIDA': 0.8,
        'NUM_PEDIDO_INTER': 0.0,#?
        'TIPO_INTERNACAO': 0.0,
        'TIPO_INTERNACAO_PEDIDO': 0.0,#?
        'NUM_SENHA_INTER': 0.0,#?
        'DT_OCORR_EVENTO': 0.8,
        'COD_ITEM_PROD_MEDICA': 1.0,
        'DS_PROD_MEDICA': 0.8,
        'TIPO_TABELA': 0.0,
        'TIPO_PROD_MEDICA': 0.8,
        'COD_PROCEDIMENTO_CM': 0.0,#?
        'QTDE_PROD_MEDICA': 1.0,
        'VLR_PROD_MEDICA': 0.8,
        'CONSELHO_SOLICITANTE': 0.0,
        'CRM_SOLICITANTE': 0.0,
        'UF_SOLICITANTE': 0.0,
        'CONSELHO_EXECUTANTE': 0.0,
        'UF_EXECUTANTE': 0.0,
        'NOME_PROCEDIMENTO_PRINCIPAL': 0.0,#?
        'CARATER_CONTA_MEDICA': 0.0,
        'NOME_EXEC_AUTORIZACAO': 0.0,#?
        'NOME_ACOMODACAO': 0.8,
        'NOME_ACOMODACAO_PEDIDO': 0.0,#?
        'NOME_TRATAMENTO_PEDIDO': 0.0,#?
        'TIPO_REGIME_PEDIDO': 0.0,
        'DS_PRESTADOR_ORIGEM': 0.0,#?
        'DS_PRESTADOR_CABECA': 0.0,#?
        'DS_PRESTADOR': 0.8,
        'CIDADE_PRESTADOR': 0.8,
        'UF_PRESTADOR': 0.8,
        'NOME_VINCULACAO': 0.0,#?
        'NOME_TIPO_PRESTADOR': 0.8,
        'GRUPO_PRESTADOR': 0.0,
        'GP_EXECUTANTE': 0.0,
        'DT_INTERNACAO': 0.0,#?
        'DT_ALTA': 0.0,#?
        'CBO_SOLICITANTE': 0.0,#?
        'CRM_EXECUTANTE': 0.0,
        'CBO_EXECUTANTE': 0.0,
        'COD_PROCEDIMENTO_PRINCIPAL': 0.0,
        'COD_EXEC_AUTORIZACAO': 0.0,#?
        'CARATER_AUTORIZACAO_PEDIDO': 0.0,#?
        'COD_ACOMODACAO_TISS': 0.0,#?
        'COD_ACOMODACAO_TISS_PEDIDO': 0.0,#?
        'COD_ENTIDADE_TS_SEGURADO': 0.0,#?
        'COD_PRESTADOR_ORIGEM': 1.0,
        'NUM_INSC_FISCAL_ORIGEM': 0.0,#?
        'TIPO_PRESTADOR_ORIGEM': 0.8,
        'COD_PRESTADOR_CABECA': 1.0,
        'NUM_INSC_FISCAL_CABECA': 0.0,#?
        'TIPO_PRESTADOR_CABECA': 0.8,
        'ID_GRUPO': 0.0,
        'CODIGO_ESTIPULANTE': 0.0,
        'NR_CONTRATO': 0.0,#?
        'REGIME_DOMICILIAR': 0.0,#?
        'NIVEL_1_GRUPO_TP_DESPESA': 0.0,#?
        'COD_PRESTADOR_TS': 1.0,
        'COD_ENT_TS_PREST': 0.0,#?
        'COD_PRESTADOR': 1.0,
        'NR_CNPJ': 0.0,#?
        'CLIENTE': 0.0,#?
    }
    print()
    data = df.replace({'.': np.nan, 'None': np.nan, ' ': np.nan, '': np.nan})
    print(data)
    columns = df.columns
    s1 = data.notna().mean()
    i = 0
    invalid_values = []
    for col in s1:
        if col < expected_completeness[columns[i]] :
            invalid_values.append([columns[i], col, expected_completeness[columns[i]]])
        print(columns[i],col,expected_completeness[columns[i]])
        i = i+1
    print(invalid_values)
    return invalid_values

# df = pd.DataFrame(data)
# df = validation_rules_DataFrame(df)
# print(df)