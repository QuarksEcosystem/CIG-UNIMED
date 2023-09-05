import streamlit as st
import streamlit_authenticator as stauth
import pandas as pd
import polars as pl
import plotly.express as px
# import re
# import validators
import time
import json
import yaml
# import streamlit.components.v1 as components
from connection.snowflakeconnection import connection, connection_report, uploadToSnowflake,verif_insert_table,consulta_snow,updateUserHistory, send_email_to_unimed
from tasks import tasks_snow
from PIL import Image
# import base64
from validacoes_pd import validation_rules_DataFrame, checa_completude
from yaml.loader import SafeLoader
# from itertools import cycle
from snowflake.snowpark.session import *
from streamlit_authenticator.hasher import Hasher
from streamlit_authenticator.authenticate import Authenticate
from Main_Page import load_data, dados_snow


def tipo_de_rede(user):
    user = dados_snow('report')
    _session = connection_report(user)
    df_full = load_data(_session)
    df = df_full.group_by('TP_REDE_VIDAS').agg(pl.col('CUSTO_POTENCIAL').sum())
    fig = px.bar(df,x='TP_REDE_VIDAS',y='CUSTO_POTENCIAL',text_auto=True)
    return fig


def tipo_acomodacao():
    user = dados_snow('report')
    _session = connection_report(user)
    df_full = load_data(_session)
    df = df_full.group_by('TP_ACOMODACAO_CIG').agg(pl.count().alias('QTD POR ACOMODAÇÃO'))
    fig = px.bar(df,x='TP_ACOMODACAO_CIG',y='QTD POR ACOMODAÇÃO',text_auto=True)
    return fig

def custo_total():
    user = dados_snow('report')
    _session = connection_report(user)
    df_full = load_data(_session)
    df = df_full['CUSTO_POTENCIAL'].agg(pl.sum()).to_pandas()['sum']
    fig = px.bar(x=['CUSTO_POTENCIAL'],y=df,text_auto=True)
    return fig