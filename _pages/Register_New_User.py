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
    # Creating a new user registration widget
    try:
        if authenticator.register_user('Register user', preauthorization=False):
            st.success('User registered successfully')
    except Exception as e:
        st.error(e)

    # Saving config file acho que esta atualizando quando o config ainda nao foi setado
    with open('config.yaml', 'w') as file:
        yaml.dump(config, file, default_flow_style=False)

