# main.py

import time
import streamlit as st
import asyncio
from auth import AuthManager
from grpc_client import GRPCClient
from message_handler import MessageHandler
from utils import initialize_session, setup_logging

logger = setup_logging()
logger.info("Aplicativo Chat Empresas iniciado.")

USER_AVATAR = "🧑‍⚕️"
BOT_AVATAR = "🤖"


def show_auth_interface() -> tuple[str, str]:
    """Exibe a interface de autenticação na sidebar e retorna a ação escolhida e o e-mail informado."""
    auth_option = st.sidebar.radio(
        "Opção de Login:",
        ["Use seu e-mail de registro", "Novo Usuário"]
    )
    user_email = st.sidebar.text_input("Digite seu e-mail")
    action = "register" if auth_option == "Novo Usuário" else "login"
    button_label = "Registrar" if action == "register" else "Login"
    if st.sidebar.button(button_label):
        return action, user_email.strip()
    return None, None


def perform_auth(auth_manager: AuthManager, action: str, email: str) -> bool:
    """Realiza o registro ou login de acordo com a ação e o e-mail fornecidos."""
    if not email:
        st.sidebar.error("Por favor, insira um e-mail válido.")
        logger.warning("Tentativa de autenticação com e-mail vazio.")
        return False

    if action == "register":
        logger.info(f"Solicitação de registro para o e-mail: {email}")
        if auth_manager.register_user(email=email):
            st.sidebar.success("Registro bem-sucedido!")
            st.session_state.is_logged_in = True
            st.session_state.useremail = email
            logger.info(f"Usuário {email} registrado e logado com sucesso.")
            return True
        else:
            st.sidebar.error("Falha no registro. O e-mail já está registrado?")
            logger.warning(f"Falha no registro para o e-mail: {email}")
            return False
    else:  # action == "login"
        logger.info(f"Solicitação de login para o e-mail: {email}")
        if auth_manager.login_user(email=email):
            st.sidebar.success("Login bem-sucedido!")
            st.session_state.is_logged_in = True
            st.session_state.useremail = email
            logger.info(f"Usuário {email} autenticado e logado com sucesso.")
            return True
        else:
            st.sidebar.error("Credenciais inválidas. Tente novamente.")
            logger.warning(f"Falha na autenticação para o e-mail: {email}")
            return False


def get_assistant_response(grpc_client: GRPCClient, question: str) -> tuple[str, float]:
    """
    Obtém a resposta do assistente via gRPC para a pergunta informada e mede o tempo de processamento.
    Retorna uma tupla com a resposta e o tempo decorrido (em segundos).
    """
    start_time = time.perf_counter()
    try:
        response = asyncio.run(grpc_client.ask_question(question))
        logger.info(f"Resposta recebida para a pergunta '{question}': {response}")
    except Exception as e:
        logger.error(f"Erro ao obter resposta para a pergunta '{question}': {e}", exc_info=True)
        response = "Desculpe, ocorreu um erro ao processar sua pergunta."
    end_time = time.perf_counter()
    processing_time = end_time - start_time
    return response, processing_time


def display_chat_history(messages: list):
    """Exibe o histórico de mensagens no chat."""
    for message in messages:
        avatar = USER_AVATAR if message["role"] == "user" else BOT_AVATAR
        with st.chat_message(message["role"], avatar=avatar):
            st.markdown(message["content"])


def chat_interface(grpc_client: GRPCClient):
    """Renderiza a interface do chat para o usuário autenticado."""
    message_handler = MessageHandler(user_email=st.session_state.useremail)
    st.sidebar.text(f"Usuário: {st.session_state.useremail}")
    st.sidebar.text(f"Sua cota de prompt é: {message_handler.get_message_limit()}")

    messages = message_handler.load_user_messages()
    display_chat_history(messages)

    user_question = st.chat_input("Como posso te ajudar?")
    if user_question:
        logger.info(f"Usuário {st.session_state.useremail} enviou uma pergunta: {user_question}")
        message_handler.save_user_message(user_question)
        with st.chat_message("user", avatar=USER_AVATAR):
            st.markdown(user_question)

        with st.chat_message("assistant", avatar=BOT_AVATAR):
            message_placeholder = st.empty()
            with st.spinner("Processando..."):
                response, processing_time = get_assistant_response(grpc_client, user_question)
            # Exibe a resposta juntamente com o tempo de processamento de forma discreta
            message_placeholder.markdown(
                f"{response}\n\n<sub>Tempo de resposta: {processing_time:.2f} segundos</sub>",
                unsafe_allow_html=True
            )
            message_handler.save_assistant_message(response)
            message_handler.update_counter()


def main():
    """Função principal que orquestra a execução da aplicação."""
    initialize_session()

    st.title("Chat Empresas - Onde podemos conversar sobre o Cadastro das empresas Brasileiras")
    st.sidebar.title("Chat X")
    st.sidebar.markdown("---")
    st.sidebar.header("Você é novo por aqui?")

    auth_manager = AuthManager()
    grpc_client = GRPCClient()

    if not st.session_state.get("is_logged_in", False):
        action, email = show_auth_interface()
        if action and email:
            perform_auth(auth_manager, action, email)

    if st.session_state.get("is_logged_in", False):
        st.sidebar.empty()  # Limpa a sidebar após o login
        logger.debug(f"Exibindo interface de chat para o usuário {st.session_state.useremail}.")
        chat_interface(grpc_client)
    else:
        st.error("Por favor, faça o login para continuar.")
        logger.info("Acesso negado: usuário não autenticado.")


if __name__ == "__main__":
    main()
