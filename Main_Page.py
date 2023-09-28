import streamlit as st
import streamlit_authenticator as stauth
import pandas as pd
import polars as pl
import plotly.express as px
# import re
# import validators
import time
import datetime
import json
import yaml
# from plot_graficos.graficos import tipo_de_rede, tipo_acomodacao, custo_total
# import streamlit.components.v1 as components
from connection.snowflakeconnection import connection, connection_report, uploadToSnowflake,verif_insert_table,consulta_snow,consulta_snow2,updateUserHistory
from tasks import tasks_snow
from PIL import Image
# import base64
from validacoes_pd import validation_rules_DataFrame, checa_completude
from yaml.loader import SafeLoader
# from itertools import cycle
from snowflake.snowpark.session import *
from streamlit_authenticator.hasher import Hasher
from streamlit_authenticator.authenticate import Authenticate
from streamlit_extras.metric_cards import style_metric_cards
import datetime
import locale
import zipfile #adicionado para atender a demanda de upload de arquivos zipados

# from streamlit_extras.colored_header import colored_header #?
# from streamlit_extras.add_vertical_space import add_vertical_space #?
# from io import StringIO


# if 'connection_established' not in st.session_state:
#     st.session_state['connection_established'] = None

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

# def upper_text(input_text):
#     input_text = input_text.upper()
#     return input_text
# def verificar_user(email_input, cnpj_input):
#     #keys = json.loads(open("./keys/key.json").read())
#     # Verificar se a chave email_input existe no dicionário keys.
#     if validate_email(email_input):
#         if validate_cnpj(cnpj_input):
#             return True
#         else:
#             st.error("CNPJ Inválida")
#     else:
#         st.error("Email Inválido")
#     return False

# def atualiza_tabela_usuario():
#     #atualiza a tabela de usuarios no snowflake com os dados do usuario atual
#     #atualizacao deve ocorrer logo antes da chamada do CIG
#     st.session_state.email, st.session_state.cnpj, st.session_state.nome, st.session_state.telefone
#     table = "HISTORICO_USUARIOS"
#     updateUserHistory(session = session, 
#                       outputTableName = "HISTORICO_USUARIOS", 
#                       database = 'UNIMED_STREAMLIT_SF', 
#                       schema = 'BLOB', 
#                       email = st.session_state.email, 
#                       cnpj = st.session_state.cnpj, 
#                       nome = st.session_state.nome, 
#                       telefone = st.session_state.telefone
#                       )
#     return 0

# def validate_email(email: str) -> bool:
#         """
#         Checks the validity of the entered email.
#         Parameters
#         ----------
#         email: str
#             The email to be validated.
#         Returns

#         -------
#         bool
#             Validity of entered email.
#         """
#         return "@" in email and 2 < len(email) < 320




# def validate_cnpj(cnpj: str) -> bool:
#     LENGTH_CNPJ = 14
#     if len(cnpj) != LENGTH_CNPJ:
#         return False

#     if cnpj in (c * LENGTH_CNPJ for c in "1234567890"):
#         return False

#     cnpj_r = cnpj[::-1]
#     for i in range(2, 0, -1):
#         cnpj_enum = zip(cycle(range(2, 10)), cnpj_r[i:])
#         dv = sum(map(lambda x: int(x[1]) * x[0], cnpj_enum)) * 10 % 11
#         if cnpj_r[i - 1:i] != str(dv % 10):
#             return False

#     return True


# Acessar os dados do snowflake referente ao user.
def dados_snow(email_input):
    keys = json.loads(open("./keys/key.json").read())
    key = keys[email_input]
    return key


# Função para exibir a seção de login.
# def show_login_section():
#     with open('config.yaml') as file:
#         config = yaml.load(file, Loader=SafeLoader)

    # Creating the authenticator object
    # authenticator = Authenticate(
    #     config['credentials'],
    #     config['cookie']['name'],
    #     config['cookie']['key'],
    #     config['cookie']['expiry_days'],
    #     config['preauthorized']
    # )
    # Cria uma seção vazia para exibir o conteúdo
    # st.markdown('''
    # ''')
    # # Cria campos de entrada para o usuário e senha
    # # name, authentication_status, username = authenticator.login('Login', 'main')
    # # if authentication_status:
    # #     st.session_state.name = name
    # #     st.session_state.connection_established = True
    # #     authenticator.logout('Logout', 'main')
    # #     st.write(f'Bem Vindo *{name}*')
    # # elif authentication_status is False:
    # #     st.session_state.connection_established = False
    # #     st.error('Username/password incorreto')
    # #     st.session_state.clear()
    # #         # Recarrega a página
    # # elif authentication_status is None:
    # #     st.session_state.connection_established = False
    # #     st.warning('Insira seu Username e Senha')


    # name, authentication_status, username = authenticator.login('Login', 'main')
    # if st.session_state['authentication_status']:
    #     authenticator.logout('Logout', 'main')
    #     st.write('Welcome *%s*' % (st.session_state['name']))
    #     st.session_state.connection_established = True
    #     st.success("Conexão estabelecida!")
    #     # Aguarda por 1 segundo antes de recarregar a página
    #     time.sleep(1)
    #     st.experimental_rerun()
    # elif st.session_state['authentication_status'] == False:
    #     st.error('Username/password is incorrect')
    # elif st.session_state['authentication_status'] == None:
    #     st.warning('Please enter your username and password')
    
    # # Creating a password reset widget
    # if authentication_status:
    #     try:
    #         if authenticator.reset_password(username, 'Reset password'):
    #             st.success('Password modified successfully')
    #     except Exception as e:
    #         st.error(e)

    # Creating a new user registration widget
    # try:
    #     if authenticator.register_user('Register user', preauthorization=False):
    #         st.success('User registered successfully')
    # except Exception as e:
    #     st.error(e)

    # Creating a forgot password widget
    # try:
    #     username_forgot_pw, email_forgot_password, random_password = authenticator.forgot_password('Forgot password')
    #     if username_forgot_pw:
    #         st.success('New password sent securely')
    #         # Random password to be transferred to user securely
    #     else:
    #         st.error('Username not found')
    # except Exception as e:
    #     st.error(e)

    # # Creating a forgot username widget
    # try:
    #     username_forgot_username, email_forgot_username = authenticator.forgot_username('Forgot username')
    #     if username_forgot_username:
    #         st.success('Username sent securely')
    #         # Username to be transferred to user securely
    #     else:
    #         st.error('Email not found')
    # except Exception as e:
    #     st.error(e)

    # # Creating an update user details widget
    # if authentication_status:
    #     try:
    #         if authenticator.update_user_details(username, 'Update user details'):
    #             st.success('Entries updated successfully')
    #     except Exception as e:
    #         st.error(e)

    # with open('./config.yaml', 'w') as file:
    #     yaml.dump(config, file, default_flow_style=False)
        
    # email_input = st.text_input("Email:", key="email_input")
    # cnpj_input = st.text_input("CNPJ (apenas os números):", key="cnpj_input")
    # nome_input = st.text_input("Nome:", key="nome_input")
    # telefone_input = st.text_input("Telefone:", key="telefone_input")

    # # Verifica se o botão "Connect" foi pressionado
    # if st.button("Connect"):
    #     # Verifica se os campos de usuário e senha foram preenchidos
    #     if email_input and cnpj_input and nome_input and telefone_input:
    #         # Verifica se o usuário e senha são válidos (chamando a função verificar_user)
    #         if verificar_user(email_input,cnpj_input):
    #             st.session_state.email = email_input
    #             st.session_state.cnpj = cnpj_input
    #             st.session_state.nome = nome_input
    #             st.session_state.telefone = telefone_input
    #             st.session_state.connection_established = True
    #             st.success("Conexão estabelecida!")
    #             # Aguarda por 1 segundo antes de recarregar a página
    #             time.sleep(1)
    #             st.experimental_rerun()
    #         # Se o usuário ou senha forem incorretos, exibe uma mensagem de aviso
    #     else:
    #         # Se os campos de usuário e senha não forem preenchidos, exibe uma mensagem de aviso
    #         st.warning("Por favor, preencha todos os campos.")
    
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
def load_data(_session, cliente):
    _session = connection_report(user)
    # cliente = st.session_state['name']
    snow_df = _session.table('AMOSTRA_RETORNO')
    snow_df = snow_df.filter(col("CLIENTE") == cliente)
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


def tipo_de_rede(dataframe):
    dataframe = dataframe.groupby('TP_REDE_VIDAS').agg(pl.col('CUSTO_POTENCIAL').sum())
    dataframe = dataframe.rename({'TP_REDE_VIDAS': 'Tipo', 'CUSTO_POTENCIAL': 'Custo Total'})
    fig = px.bar(dataframe, y='Tipo', x='Custo Total', orientation='h', text_auto=True, title='Tipo de Rede', color_discrete_sequence=[px.colors.qualitative.T10[4]])
    return fig


def tipo_acomodacao(dataframe):
    dataframe = dataframe.groupby('TP_ACOMODACAO_CIG').agg(pl.col('ID_PESSOA').count())
    dataframe = dataframe.rename({'ID_PESSOA':'Quantidade', 'TP_ACOMODACAO_CIG': 'Acomodação CIG'})
    fig = px.pie(dataframe, values='Quantidade', names='Acomodação CIG', title='Tipo Acomodação', color_discrete_sequence=[px.colors.qualitative.Dark2[1], 
                                                                                                                           px.colors.qualitative.Set2[1],
                                                                                                                           px.colors.qualitative.Pastel2[1]])
    fig.update_traces(hole=.6)
    return fig


def evolucao_custo_total(dataframe):
    dataframe = dataframe.groupby('Ano').agg(pl.col('CUSTO_POTENCIAL').sum())
    dataframe = dataframe.rename({'CUSTO_POTENCIAL':'Custo Total'})
    dataframe = dataframe.sort(pl.col('Ano'))
    fig = px.bar(dataframe, x='Ano', y='Custo Total', text_auto=True, title='Evolução do Custo Total', color_discrete_sequence=[px.colors.qualitative.T10[4]])
    fig.update_xaxes(type='category')
    return fig


def evolucao_qtde_atendimento(dataframe):
    dataframe = dataframe.groupby('Ano').agg(pl.col('ID_PESSOA').count().alias('Quantidade'))
    dataframe = dataframe.sort(pl.col('Ano'))
    fig = px.bar(dataframe, x='Quantidade', y='Ano',orientation='h',text_auto=True, title='Evolução da Qtde de Atendimento', color_discrete_sequence=[px.colors.qualitative.Dark2[1]])
    fig.update_yaxes(type='category')
    return fig


def evolucao_custo_medio(dataframe):
    dataframe = dataframe.groupby('Ano').agg(pl.col('CUSTO_POTENCIAL').mean())
    dataframe = dataframe.rename({'CUSTO_POTENCIAL': 'Custo Médio'})
    dataframe = dataframe.sort(pl.col('Ano'))
    fig = px.bar(dataframe, x='Ano', y='Custo Médio',text_auto=True, title='Evolução do Custo Médio', color_discrete_sequence=[px.colors.qualitative.T10[4]])
    fig.update_xaxes(type='category')
    return fig


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
        # # Título da aplicação na Main Page
        # st.title('Snowflake-Streamlit App')
        # # Verifica se a conexão ainda não foi estabelecida ou se não está estabelecida
        # if 'connection_established' not in st.session_state or not st.session_state.connection_established:
        #     show_login_section()
        #     st.write("Para estabelecer conexão, realize o login:")
        # else:
        st.markdown("<br><br>", unsafe_allow_html=True)
        # Divide a coluna da barra lateral em duas partes
        col1, col2 = st.columns(2)
        with col1:
            image = Image.open('./img/icon-g3e57076af_1280.png')
            st.image(image, width=100,output_format='PNG', use_column_width=False)
        with col2:
            # Exibe o cabeçalho com o email do usuário
            # st.header(st.session_state['nome'])
            user_name = st.session_state['login_name'][0][0]
            st.write(f"Bem vindo(a): {user_name.upper()}")
            opcoes_menu = ['Entrada de dados', 'Consulta', 'Gráficos']
            # Campo de menu
            pagina_selecionada = st.selectbox('Menu', opcoes_menu)
        # Botão 'Sair' na barra lateral
        # if st.session_state['authentication_status'] == False:
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
                    dataframes = []
                    df = None
                    # Verifica se um arquivo foi carregado
                    if uploaded_file is not None:
                        print(uploaded_file.type)
                    # if uploaded_file:
                        if uploaded_file.type == 'application/zip':
                            print('arquivo zippado')
                            # Open the ZIP file and extract CSV files
                            with zipfile.ZipFile(uploaded_file, 'r') as zip_file:
                                # List all the files in the ZIP archive
                                zip_file_list = zip_file.namelist()
                                
                                # Iterate through each file in the ZIP archive
                                for file_name in zip_file_list:
                                    # Check if the file has a .csv extension
                                    if file_name.endswith('.csv'):
                                        # Extract the file
                                        with zip_file.open(file_name) as csv_file:
                                            # Read the CSV file into a DataFrame
                                            df = pd.read_csv(csv_file, sep=',', dtype={'COD_PREST': str}, on_bad_lines='skip')#adicionei isso para rodar os testes, o que fazer quando tiver linhas com numeros diferentes de colunas
                                            # Append the DataFrame to the list
                                            dataframes.append(df)

                            # Concatenate all the DataFrames into a single DataFrame
                            df = pd.concat(dataframes, ignore_index=True)
                            st.dataframe(df.head(10))
                            
                        # Verifica se o tipo do arquivo é 'text/csv'
                        elif uploaded_file.type == 'text/csv':
                            # Lê o conteúdo do arquivo em formato de string
                            # stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
                            # string_data = stringio.read()
                            # Carrega os dados em um DataFrame

                            ###Criar validação para checar o separador
                            df = pd.read_csv(uploaded_file, sep=',', dtype={'COD_PREST': str}, on_bad_lines='skip')
                        if df is not None:
                            df = df.loc[:, ~df.columns.str.contains('Unnamed: 0')]
                            #df['CLIENTE'] = st.session_state['cliente'][0][0]

                            # read = pd.read_csv(uploaded_file, chunksize=1000000, encoding='latin1', sep=';', dtype={'COD_PREST': str}, low_memory=False)
                            # Exibe o DataFrame
                            # for chunk in read:
                                    # Processa cada chunk conforme necessário
                                # (exemplo: realizar operações, transformações, etc.)
                                # Exemplo simples: imprimir o chunk
                            st.dataframe(df.head(10))
                                # break
                            # Define o modo de inserção como 'Append'
                            Mode = 'Append'
                            # Botão 'Upload' para carregar os dados para o Snowflake
                            if st.button('Upload'):
                                #valida_complitude
                                #validacao_complitude = checa_completude(df)
                                #validacao_preliminar = validation_rules_DataFrame(df)
                                # if len(validacao_complitude) > 0:
                                #     error_message = "Arquivo com muitos linhas vazias nas colunas:  \n"
                                #     for coluna in validacao_complitude:
                                #         error_message += coluna[0] + ': ' + str(coluna[1]*100) + '\%'+' de linhas preenchidas. Minimo aceitavel : ' + str(coluna[2]*100) + '\%' + '  \n'
                                #     st.error(error_message)
                                #valida_complitude
                                #el
                                # if validation_rules_DataFrame(df) is not None:
                                #     st.error(validacao_preliminar)
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
                                    target_table = 'INPUT_'+st.session_state['cliente'][0][0]+'_'+datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
                                    uploadToSnowflake(df=df,
                                                        session = session,
                                                        outputTableName = target_table,
                                                        database = 'UNIMED_STREAMLIT_SF',
                                                        schema = 'BLOB',
                                                        temporary=True)
                                    # Executa tarefas no Snowflake
                                    ##tasks_snow(session)
                                    st.success("Arquivo carregado com sucesso!")
                                    #send_email_to_unimed(session)

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
                            label='Download output1',
                            data=df.to_csv().encode('utf-8'),
                            file_name='data_frame.csv',
                            mime='text/csv'
                            )
                    df = consulta_snow2(session,cliente)
                    st.dataframe(df.head(10)) 
                    st.download_button(
                            label='Download output2',
                            data=df.to_csv().encode('utf-8'),
                            file_name='data_frame_2.csv',
                            mime='text/csv'
                            )
                elif pagina_selecionada == 'Gráficos':
                    user = dados_snow('report')
                    session = connection_report(user)

                    dados = load_data(session, st.session_state['cliente'][0][0])

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
                                        'CATEGORIAS_CIG', 'TIPO_INTERNACAO_CIG', 'REGIAO_VIDAS', 'CUSTO_POTENCIAL', 'ANO', 'Mês'])
                    dados = dados.rename({'ANO': 'Ano', 'REGIAO_VIDAS': 'Região'})
                    
                    dados = dados.with_columns(pl.col('Ano').cast(pl.Int64))

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
                    col_a1, col_a2= st.columns(2)

                    custo_total = dados_filtrado.select(pl.sum('CUSTO_POTENCIAL'))['CUSTO_POTENCIAL'][0]
                    custo_total = locale.format_string("%.2f", custo_total, grouping=True)

                    custo_medio = dados_filtrado.select(pl.mean('CUSTO_POTENCIAL'))['CUSTO_POTENCIAL'][0]
                    custo_medio = locale.format_string("%.2f", custo_medio, grouping=True)

                    qtde_atendimento = len(dados_filtrado)
                    qtde_atendimento = locale.format_string("%d", qtde_atendimento, grouping=True)

                    contagem_cod_cig = dados_filtrado['COD_CIG'].n_unique()
                    contagem_cod_cig = locale.format_string("%d", contagem_cod_cig, grouping=True)
                    
                    col_a1.metric('Custo Total', value=f'R${custo_total}')
                    col_a1.metric('Custo Médio', value=f'R${custo_medio}')

                    col_a2.metric('Qtde Atendimento', value=qtde_atendimento)
                    col_a2.metric('Contagem cod_CIG', value=contagem_cod_cig)

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


                    cigs = dados_filtrado.groupby('DESC_CIG').agg(
                       pl.sum('CUSTO_POTENCIAL').alias('Custo Total'),
                       pl.mean('CUSTO_POTENCIAL').alias('Custo Médio'),
                       pl.count().alias('Qtde Atendimento')
                   )
                    st.dataframe(cigs, use_container_width=True)
                    
                    # fig = px.colors.qualitative.swatches()
                    # st.plotly_chart(fig)

                    
                    # st.success("Aqui está os dados: ")
                    # user = dados_snow('report')
                    # session = connection_report(user)
                    # df = consulta_snow(session)
                    # st.dataframe(df, height=900)
                    # st.download_button(
                    #         label='Download',
                    #         data=df.to_csv().encode('utf-8'),
                    #         file_name='data_frame.csv',
                    #         mime='text/csv'
                    #         )