# utils.py

import streamlit as st
import logging
from logging.handlers import RotatingFileHandler
import os


def initialize_session():
    """Inicializa variáveis de sessão padrão no Streamlit.

    Configura o estado da sessão com valores padrão para autenticação e mensagens.
    """
    if "is_logged_in" not in st.session_state:
        st.session_state["is_logged_in"] = False
    if "messages" not in st.session_state:
        st.session_state["messages"] = []
    if "useremail" not in st.session_state:
        st.session_state["useremail"] = ""
    if "thread_key" not in st.session_state:
        st.session_state["thread_key"] = ""


def setup_logging() -> logging.Logger:
    """Configura o logger para o aplicativo Chat X.

    Configura handlers para saída no console e em arquivos de log, com rotação de arquivos.

    Returns:
        logging.Logger: O logger configurado para o aplicativo.
    """
    logger = logging.getLogger("chat_app")
    logger.setLevel(logging.DEBUG)  # Defina o nível de log conforme necessário

    # Formato do log
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        # Handler para saída no console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler para arquivos de log
        if not os.path.exists("logs"):
            os.makedirs("logs")
        file_handler = RotatingFileHandler(
            "logs/chat_app.log", maxBytes=5*1024*1024, backupCount=2
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.error(f"Erro ao configurar logging: {e}", exc_info=True)
        # Dependendo da criticidade, pode re-raise a exceção ou lidar de outra forma

    return logger
