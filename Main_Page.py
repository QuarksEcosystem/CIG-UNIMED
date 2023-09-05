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
# from plot_graficos.graficos import tipo_de_rede, tipo_acomodacao, custo_total
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

# from streamlit_extras.colored_header import colored_header #?
# from streamlit_extras.add_vertical_space import add_vertical_space #?
# from io import StringIO



if 'connection_established' not in st.session_state:
    st.session_state['connection_established'] = None
# Configurando o ambiente do streamlit
st.set_page_config(page_title="Aplicação CIG",
                   page_icon="💻",
                   layout="centered",
                   initial_sidebar_state="auto")

# Função para exibir a seção de login.
def check_timeout():
    #checa a tabela e verifica se a ultima vez que o usuario utilizou o serviço faz mais tempo que o timeout
    timeout = 60
    return -1

def upper_text(input_text):
    input_text = input_text.upper()
    return input_text
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
def show_login_section():
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)

    # Creating the authenticator object
    authenticator = Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )
    # Cria uma seção vazia para exibir o conteúdo
    st.markdown('''
    ''')
    # Cria campos de entrada para o usuário e senha
    # name, authentication_status, username = authenticator.login('Login', 'main')
    # if authentication_status:
    #     st.session_state.name = name
    #     st.session_state.connection_established = True
    #     authenticator.logout('Logout', 'main')
    #     st.write(f'Bem Vindo *{name}*')
    # elif authentication_status is False:
    #     st.session_state.connection_established = False
    #     st.error('Username/password incorreto')
    #     st.session_state.clear()
    #         # Recarrega a página
    # elif authentication_status is None:
    #     st.session_state.connection_established = False
    #     st.warning('Insira seu Username e Senha')


    name, authentication_status, username = authenticator.login('Login', 'main')
    if st.session_state['authentication_status']:
        authenticator.logout('Logout', 'main')
        st.write('Welcome *%s*' % (st.session_state['name']))
        st.session_state.connection_established = True
        st.success("Conexão estabelecida!")
        # Aguarda por 1 segundo antes de recarregar a página
        time.sleep(1)
        st.experimental_rerun()
    elif st.session_state['authentication_status'] == False:
        st.error('Username/password is incorrect')
    elif st.session_state['authentication_status'] == None:
        st.warning('Please enter your username and password')
    
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

    with open('./config.yaml', 'w') as file:
        yaml.dump(config, file, default_flow_style=False)
        
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
    
    

@st.cache_data
def load_data(_session):
    _session = connection_report(user)
    # cliente = st.session_state['name']
    snow_df = _session.table('CORTADA_AMOSTRA_RETORNO')
    pl_df = pl.from_pandas(snow_df.to_pandas())
    return pl_df


def tipo_de_rede():
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
    qtd_df = len(df_full)
    df = df.group_by("TP_ACOMODACAO_DETALHEVIDAS").agg([pl.count().alias("count")]).with_columns((pl.col("count") / pl.sum("count")).alias("percent_count"))
    fig = px.bar(df,x='TP_ACOMODACAO_CIG',y='QTD POR ACOMODAÇÃO',text_auto=True)
    return fig



with st.sidebar:
    # Título da aplicação na Main Page
    st.title('Snowflake-Streamlit App')
    # Verifica se a conexão ainda não foi estabelecida ou se não está estabelecida
    if 'connection_established' not in st.session_state or not st.session_state.connection_established:
        show_login_section()
        st.write("Para estabelecer conexão, realize o login:")
    else:
        st.markdown("<br><br>", unsafe_allow_html=True)
        # Divide a coluna da barra lateral em duas partes
        col1, col2 = st.columns(2)
        with col1:
            image = Image.open('./img/icon-g3e57076af_1280.png')
            st.image(image, width=100,output_format='PNG', use_column_width=False)
        with col2:
            # Exibe o cabeçalho com o email do usuário
            # st.header(st.session_state['nome'])
            user_name = st.session_state['name']
            st.write(f"Bem vindo(a): {upper_text(user_name)}")
            opcoes_menu = ['Entrada de dados', 'Consulta', 'Gráficos']
            # Campo de menu
            pagina_selecionada = st.selectbox('Menu', opcoes_menu)
        # Botão 'Sair' na barra lateral
        # if st.session_state['authentication_status'] == False:
        if st.button('Sair'):
            Authenticate.logout(st.session_state['authentication_status'] == True,'Logout','sidebar')
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
                        # Lê o conteúdo do arquivo em formato de string
                        # stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
                        # string_data = stringio.read()
                        # Carrega os dados em um DataFrame

                        ###Criar validação para checar o separador
                        df = pd.read_csv(uploaded_file, sep=',', dtype={'COD_PREST': str}, nrows=5)
                        df = df.loc[:, ~df.columns.str.contains('Unnamed: 0')]

                        # read = pd.read_csv(uploaded_file, chunksize=1000000, encoding='latin1', sep=';', dtype={'COD_PREST': str}, low_memory=False)
                        # Exibe o DataFrame
                        # for chunk in read:
                                # Processa cada chunk conforme necessário
                            # (exemplo: realizar operações, transformações, etc.)
                            # Exemplo simples: imprimir o chunk
                        st.dataframe(df)
                            # break
                        # Define o modo de inserção como 'Append'
                        Mode = 'Append'
                        # Botão 'Upload' para carregar os dados para o Snowflake
                        if st.button('Upload'):
                            #valida_complitude
                            validacao_complitude = checa_completude(df)
                            validacao_preliminar = validation_rules_DataFrame(df)
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
                st.write(st.session_state['name'])
                session = connection(user)
                df = consulta_snow(session)
                st.dataframe(df, height=900)
                st.download_button(
                        label='Download',
                        data=df.to_csv().encode('utf-8'),
                        file_name='data_frame.csv',
                        mime='text/csv'
                        )
            elif pagina_selecionada == 'Gráficos':
                user = dados_snow('report')
                session = connection_report(user)
                st.plotly_chart(tipo_de_rede(),use_container_width=True)
                st.plotly_chart(tipo_acomodacao(),use_container_width=True)
                
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