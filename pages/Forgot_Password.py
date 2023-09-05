import yaml
import streamlit as st
from yaml.loader import SafeLoader
import streamlit.components.v1 as components

from streamlit_authenticator.hasher import Hasher
from streamlit_authenticator.authenticate import Authenticate

_RELEASE = False

if not _RELEASE:
    # hashed_passwords = Hasher(['abc', 'def']).generate()
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
    # Creating a forgot password widget
    try:
        username_forgot_pw, email_forgot_password, random_password = authenticator.forgot_password('Forgot password')
        print(username_forgot_pw, email_forgot_password, random_password)
        if username_forgot_pw:
            st.success('New password sent securely')
            # Random password to be transferred to user securely
        else:
            st.error('Username not found')
    except Exception as e:
        st.error(e)

    # Creating a forgot username widget
    # try:
    #     username_forgot_username, email_forgot_username = authenticator.forgot_username('Forgot username')
    #     print(username_forgot_username, email_forgot_username )
    #     if username_forgot_username:
    #         st.success('Username sent securely')
    #         # Username to be transferred to user securely
    #     else:
    #         st.error('Email not found')
    # except Exception as e:
    #     st.error(e)
    
    # Saving config file acho que esta atualizando quando o config ainda nao foi setado
    with open('config.yaml', 'w') as file:
        yaml.dump(config, file, default_flow_style=False)

    # # Saving config file
    # with open('config.yaml', 'w') as file:
    #     yaml.dump(st.session_state.config, file, default_flow_style=False)

    # Alternatively you may use st.session_state['name'], st.session_state['authentication_status'], 
    # and st.session_state['username'] to access the name, authentication_status, and username. 

    # if st.session_state['authentication_status']:
    #     authenticator.logout('Logout', 'main')
    #     st.write(f'Welcome *{st.session_state["name"]}*')
    #     st.title('Some content')
    # elif st.session_state['authentication_status'] is False:
    #     st.error('Username/password is incorrect')
    # elif st.session_state['authentication_status'] is None:
    #     st.warning('Please enter your username and password')
