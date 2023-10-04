import streamlit as st
import polars as pl
import plotly.express as px


def tipo_de_rede(dataframe):
    dataframe = dataframe.group_by('TP_REDE_VIDAS').agg(pl.col('VLR_PROD_MEDICA').sum())
    dataframe = dataframe.rename({'TP_REDE_VIDAS': 'Tipo', 'VLR_PROD_MEDICA': 'Custo Total'})
    fig = px.bar(dataframe, y='Tipo', x='Custo Total', orientation='h', text_auto=True, title='Tipo de Rede', color_discrete_sequence=[px.colors.qualitative.T10[4]])
    return fig


def tipo_acomodacao(dataframe):
    dataframe = dataframe.group_by('TP_ACOMODACAO_CIG').agg(pl.col('ID_PESSOA').count())
    dataframe = dataframe.rename({'ID_PESSOA':'Quantidade', 'TP_ACOMODACAO_CIG': 'Acomodação CIG'})
    fig = px.pie(dataframe, values='Quantidade', names='Acomodação CIG', title='Tipo Acomodação', color_discrete_sequence=[px.colors.qualitative.Dark2[1], 
                                                                                                                           px.colors.qualitative.Set2[1],
                                                                                                                           px.colors.qualitative.Pastel2[1]])
    fig.update_traces(hole=.6)
    return fig


def evolucao_custo_total(dataframe):
    dataframe = dataframe.group_by('Ano').agg(pl.col('VLR_PROD_MEDICA').sum())
    dataframe = dataframe.rename({'VLR_PROD_MEDICA':'Custo Total'})
    dataframe = dataframe.sort(pl.col('Ano'))
    fig = px.bar(dataframe, x='Ano', y='Custo Total', text_auto=True, title='Evolução do Custo Total', color_discrete_sequence=[px.colors.qualitative.T10[4]])
    fig.update_xaxes(type='category')
    return fig


def evolucao_qtde_atendimento(dataframe):
    dataframe = dataframe.group_by('Ano').agg(pl.col('ID_PESSOA').count().alias('Quantidade'))
    dataframe = dataframe.sort(pl.col('Ano'))
    fig = px.bar(dataframe, x='Quantidade', y='Ano',orientation='h',text_auto=True, title='Evolução da Qtde de Atendimento', color_discrete_sequence=[px.colors.qualitative.Dark2[1]])
    fig.update_yaxes(type='category')
    return fig


def evolucao_custo_medio(dataframe):
    dataframe = dataframe.group_by('Ano').agg(pl.col('VLR_PROD_MEDICA').mean())
    dataframe = dataframe.rename({'VLR_PROD_MEDICA': 'Custo Médio'})
    dataframe = dataframe.sort(pl.col('Ano'))
    fig = px.bar(dataframe, x='Ano', y='Custo Médio',text_auto=True, title='Evolução do Custo Médio', color_discrete_sequence=[px.colors.qualitative.T10[4]])
    fig.update_xaxes(type='category')
    return fig