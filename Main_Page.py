import streamlit as st
import streamlit_authenticator as stauth
import pandas as pd
import polars as pl
import plotly.express as px
import time
import datetime
import json
import yaml
from connection.snowflakeconnection import connection, connection_report, uploadToSnowflake,verif_insert_table,consulta_snow,updateUserHistory
from tasks import tasks_snow
from PIL import Image
# import base64
from validacoes_pd import validation_rules_DataFrame, checa_completude
from yaml.loader import SafeLoader
from snowflake.snowpark.session import *
from streamlit_authenticator.hasher import Hasher
from streamlit_authenticator.authenticate import Authenticate
from streamlit_extras.metric_cards import style_metric_cards
import locale
from plot_graficos.graficos import tipo_de_rede, tipo_acomodacao, evolucao_custo_total, evolucao_qtde_atendimento, evolucao_custo_medio

# Configurando o ambiente do streamlit
st.set_page_config(page_title="Aplicação CIG",
                   page_icon="💻",
                   layout="wide",
                   initial_sidebar_state="auto")

locale.setlocale(locale.LC_NUMERIC, 'pt_BR')

# Função para exibir a seção de login.
def check_timeout():
    #checa a tabela e verifica se a ultima vez que o usuario utilizou o serviço faz mais tempo que o timeout
    timeout = 60
    return -1


# Acessar os dados do snowflake referente ao user.
def dados_snow(email_input):
    keys = json.loads(open("./keys/key.json").read())
    key = keys[email_input]
    return key

  
@st.cache_resource
def create_session_object():
    connection_parameters = {
      "account": "zd27083.us-east-2.aws",
      "user": "GUSTAVOCIRILO",
      "password": "41593055Gu@",
      "role": "RL_POC",
      "warehouse": "WH_POC",
      "database": "UNIMED_STREAMLIT_SF",
      "schema": "STATA",
      "client_session_keep_alive": True
   }
    session = Session.builder.configs(connection_parameters).create()
    return session    


@st.cache_data
def load_data(_session, tabela):
    _session = connection_report(user)
    # cliente = st.session_state['name']
    snow_df = _session.table(tabela)
    pl_df = pl.from_pandas(snow_df.to_pandas())
    return pl_df


def login(session_login):
    passphrase = 'xYc6RYado9qk86a'
    st.markdown("<h1 style='text-align: center; color: black;'>Login</h1>", unsafe_allow_html=True)
    with st.form(key='login_form'):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit_button = st.form_submit_button(label='Login')
    if submit_button:
        st.session_state['login_username'] = session_login.sql(f"SELECT USERNAME FROM UNIMED_STREAMLIT_SF.STATA.ENCRYPTED_USERS WHERE USERNAME = '{username.upper()}';").collect()
        st.session_state['login_password'] = session_login.sql(f"SELECT TO_VARCHAR(DECRYPT(PASSWORD, '{passphrase}'), 'utf-8') FROM UNIMED_STREAMLIT_SF.STATA.ENCRYPTED_USERS WHERE USERNAME = '{username.upper()}';").collect()
        st.session_state['login_name'] = session_login.sql(f"SELECT NAME FROM UNIMED_STREAMLIT_SF.STATA.ENCRYPTED_USERS WHERE USERNAME = '{username.upper()}';").collect()
        st.session_state['email'] = session_login.sql(f"SELECT EMAIL FROM UNIMED_STREAMLIT_SF.STATA.ENCRYPTED_USERS WHERE USERNAME = '{username.upper()}';").collect()
        st.session_state['cliente'] = session_login.sql(f"SELECT CLIENTE FROM UNIMED_STREAMLIT_SF.STATA.ENCRYPTED_USERS WHERE USERNAME = '{username.upper()}';").collect()
        if len(st.session_state['login_username']) == 0:
            st.warning('Usuário não existe') 
            time.sleep(3)
            st.experimental_rerun()
        elif password != st.session_state['login_password'][0][0]:
            st.warning('Senha incorreto')
            time.sleep(2)
            st.experimental_rerun()
        else:
            st.success('Login realizado com sucesso')
            st.session_state.connection_established = True
            time.sleep(2)
            st.experimental_rerun()
    if 'button_pressed' not in st.session_state:
        st.session_state['button_pressed'] = False


def send_email_to_unimed(session : Session):
    user_email = st.session_state['email'][0][0]
    user_cliente = st.session_state['cliente'][0][0]
    send_email = session.call("SEND_EMAIL_NOTIFICATION",user_email,user_cliente,datetime.datetime.now())    
    return send_email       


session_login = create_session_object()

if 'connection_established' not in st.session_state or not st.session_state.connection_established:
    with st.sidebar:
        show = login(session_login)
elif 'connection_established' in st.session_state:
    with st.sidebar:
        st.markdown("<br><br>", unsafe_allow_html=True)
        # Divide a coluna da barra lateral em duas partes
        col1, col2 = st.columns(2)
        with col1:
            image = Image.open('./img/icon-g3e57076af_1280.png')
            st.image(image, width=100,output_format='PNG', use_column_width=False)
        with col2:
            user_name = st.session_state['login_name'][0][0]
            st.write(f"Bem vindo(a): {user_name.upper()}")
            opcoes_menu = ['Entrada de dados', 'Consulta', 'Gráficos']
            # Campo de menu
            pagina_selecionada = st.selectbox('Menu', opcoes_menu)
        if st.button('Sair'):
            st.experimental_set_query_params()
            time.sleep(1)
            st.session_state.clear()
            st.experimental_rerun()


    with st.container():
            # Verifica se a conexão foi estabelecida e se o usuário é 'admin'
            ###Criar uma função para isso
            if 'connection_established' in st.session_state and st.session_state.connection_established:
                if pagina_selecionada == "Entrada de dados":

                    # Exibe uma mensagem de sucesso
                    st.success("Carregue arquivo:")
                    # Campo para fazer upload do arquivo
                    uploaded_file = st.file_uploader("Choose a file")
                    # Verifica se um arquivo foi carregado
                    if uploaded_file is not None:
                    # if uploaded_file:
                        # Verifica se o tipo do arquivo é 'text/csv'
                        if uploaded_file.type == 'text/csv':


                            ###Criar validação para checar o separador
                            df = pd.read_csv(uploaded_file, sep=',', dtype={'COD_PREST': str})
                            df = df.loc[:, ~df.columns.str.contains('Unnamed: 0')]
                            df['CLIENTE'] = st.session_state['cliente'][0][0]

                            st.dataframe(df.head(10))
                                # break
                            # Define o modo de inserção como 'Append'
                            Mode = 'Append'
                            # Botão 'Upload' para carregar os dados para o Snowflake
                            if st.button('Upload'):
                                #valida_complitude
                                validacao_complitude = checa_completude(df)
                                validacao_preliminar = validation_rules_DataFrame(df)
                                if 1 < 0 :
                                    print('Apagar isso e tirar o comentario das validações')
                                # Obtém as informações do usuário para conexão ao Snowflake
                                #checa se a ultima vez que essa cnpj rodou
                                timeout_left = check_timeout() 
                                if timeout_left > 0: 
                                    st.error('Faltam ' + timeout_left + ' minutos para liberar novo processamento.')
                                else:
                                    user = dados_snow('admin')
                                    # Estabelece a sessão de conexão com o Snowflake
                                    session = connection(user)
                                    # atualiza_tabela_usuario()
                                    # Carrega os dados para o Snowflake
                                    uploadToSnowflake(df=df,
                                                        session = session,
                                                        outputTableName = 'SEGUROS1ANOCOMPLETO_RAW',
                                                        database = 'UNIMED_STREAMLIT_SF',
                                                        schema = 'BLOB',
                                                        temporary=True)
                                    # Executa tarefas no Snowflake
                                    tasks_snow(session)
                                    st.success("Arquivo carregado com sucesso!")
                                    send_email_to_unimed(session)

                elif pagina_selecionada == 'Consulta':
                    st.success("Aqui está os dados: ")
                    user = dados_snow('admin')
                    # Estabelece a sessão de conexão com o Snowflake
                    st.write(st.session_state['login_name'][0][0])
                    session = connection(user)
                    cliente = st.session_state['cliente'][0][0]
                    df = consulta_snow(session,cliente)
                    st.dataframe(df.head(10))
                    st.download_button(
                            label='Download',
                            data=df.to_csv().encode('utf-8'),
                            file_name='data_frame.csv',
                            mime='text/csv'
                            )
                elif pagina_selecionada == 'Gráficos':
                    tabelas = session_login.sql('SHOW TABLES;').collect()
                    tabelas = tabelas[:]
                    
                    df_tabelas = session_login.create_dataframe(tabelas)
                    df_tabelas = df_tabelas[['NAME']]
                    df_tabelas = df_tabelas.to_pandas()
                    df_tabelas = df_tabelas[df_tabelas['NAME'].str.contains(st.session_state['cliente'][0][0])]
                    
                    options = df_tabelas['NAME'].unique()
                    option = st.selectbox("Escolha a tabela", options=options)
                    
                    user = dados_snow('report')
                    session = connection_report(user)

                    dados = load_data(session, option)

                    dados = dados.with_columns(
                        pl.col('DATA_SAIDA').map_elements(lambda x: x.replace(' ', '-'))
                    )
                    dados = dados.with_columns(
                            pl.col("DATA_SAIDA")
                            .str.to_datetime()
                    )
                    dados = dados.with_columns(
                            pl.col("DATA_SAIDA")
                            .dt.strftime("%m")
                            .alias("Mês")
                    )
                    dados = dados.select(['TIPO_ATENDIMENTO', 'ID_PESSOA', 'COD_CIG', 'SEXO_CLIENTE_VIDAS', 'TP_PRODUTO_VIDAS', 'TP_REDE_VIDAS', 'TP_ACOMODACAO_CIG', 'DESC_CIG',
                                          'CATEGORIAS_CIG', 'TIPO_INTERNACAO_CIG', 'REGIAO_VIDAS', 'VLR_PROD_MEDICA', 'DEFINIDORES_CTI_VIDAS', 'OBITO_VIDAS', 'ANO', 'Mês'])
                    dados = dados.rename({'ANO': 'Ano', 'REGIAO_VIDAS': 'Região'})
                    
                    dados = dados.with_columns(pl.col('Ano').cast(pl.Int64), 
                                               pl.col('VLR_PROD_MEDICA').cast(pl.Float64),
                                               pl.col('DEFINIDORES_CTI_VIDAS').cast(pl.Int64),
                                               pl.col('OBITO_VIDAS').cast(pl.Int64)
                                )

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.image('img\Logo-Faculdade Unimed-1.png')
                    with col2:
                        st.markdown("<h1 style='text-align: center; color: black;'>Paronama Geral CIG</h1>", unsafe_allow_html=True)
                    

                    filtro_ano= st.multiselect(
                        label="Filtro para a coluna Ano",
                        options=dados['Ano'].unique().to_list(),
                        default=dados['Ano'].unique().to_list()
                    )
                    
                    filtro_regiao= st.multiselect(
                        label="Filtro para a coluna Região",
                        options=['NORTE', 'NORDESTE', 'SUDESTE', 'SUL', 'CENTRO OESTE', 'NAO IDENTIFICADO'],
                        default=['NORTE', 'NORDESTE', 'SUDESTE', 'SUL', 'CENTRO OESTE', 'NAO IDENTIFICADO']
                    )

                    dados_filtrado = dados.filter(
                                               (pl.col("Ano").is_in(filtro_ano)) &
                                               (pl.col("Região").is_in(filtro_regiao))
                                       )

                    #Baloes - cards             
                    col_a1, col_a2, col_a3= st.columns(3)

                    custo_total = dados_filtrado.select(pl.sum('VLR_PROD_MEDICA'))['VLR_PROD_MEDICA'][0]
                    custo_total = locale.format_string("%.2f", custo_total, grouping=True)

                    custo_medio = dados_filtrado.select(pl.mean('VLR_PROD_MEDICA'))['VLR_PROD_MEDICA'][0]
                    custo_medio = locale.format_string("%.2f", custo_medio, grouping=True)

                    qtde_atendimento = len(dados_filtrado)
                    qtde_atendimento = locale.format_string("%d", qtde_atendimento, grouping=True)

                    contagem_cod_cig = dados_filtrado['COD_CIG'].n_unique()
                    contagem_cod_cig = locale.format_string("%d", contagem_cod_cig, grouping=True)

                    contagem_id_pessoa = dados_filtrado['ID_PESSOA'].n_unique()
                    contagem_id_pessoa = locale.format_string("%d", contagem_id_pessoa, grouping=True)

                    qtde_cti = dados_filtrado.select(pl.sum('DEFINIDORES_CTI_VIDAS'))['DEFINIDORES_CTI_VIDAS'][0]
                    pct_cti = round((qtde_cti / int(qtde_atendimento.replace('.', ''))) * 100, 2)

                    qtde_obitos = dados_filtrado.select(pl.sum('OBITO_VIDAS'))['OBITO_VIDAS'][0]
                    pct_obitos = round((qtde_obitos / int(qtde_atendimento.replace('.', ''))) * 100, 2)

                    col_a1.metric('Custo Total', value=f'R${custo_total}')
                    col_a1.metric('Custo Médio', value=f'R${custo_medio}')

                    col_a2.metric('Qtde Atendimento', value=qtde_atendimento)
                    col_a2.metric('Contagem cod_CIG', value=contagem_cod_cig)

                    col_a3.metric('% CTI', value=f'{pct_cti}%')
                    col_a3.metric('Mortalidade', value=f'{pct_obitos}%')

                    style_metric_cards(
                        background_color="#FFF",
                        border_size_px= 1,
                        border_color="#CCC",
                        border_radius_px=5,
                        border_left_color="#D95F02",
                        box_shadow= True   
                    )

                    col_b1, col_b2, col_b3 = st.columns(3)

                    with col_b1:
                        st.plotly_chart(evolucao_custo_total(dados_filtrado), use_container_width=True)
                    with col_b2:
                        st.plotly_chart(evolucao_qtde_atendimento(dados_filtrado), use_container_width=True)
                    with col_b3:
                        st.plotly_chart(evolucao_custo_medio(dados_filtrado), use_container_width=True)

                    col_c1, col_c2 = st.columns(2)

                    with col_c1:
                        st.plotly_chart(tipo_acomodacao(dados_filtrado), use_container_width=True)
                    with col_c2:
                        st.plotly_chart(tipo_de_rede(dados_filtrado), use_container_width=True)


                    cigs = dados_filtrado.group_by('DESC_CIG').agg(
                       pl.sum('VLR_PROD_MEDICA').alias('Custo Total'),
                       pl.mean('VLR_PROD_MEDICA').alias('Custo Médio'),
                       pl.count().alias('Qtde Atendimento')
                   )
                    st.dataframe(cigs, use_container_width=True)
                    
                    # fig = px.colors.qualitative.swatches()
                    # st.plotly_chart(fig)