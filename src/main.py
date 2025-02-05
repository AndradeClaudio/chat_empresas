# main.py

import streamlit as st
import asyncio
import logging
from auth import AuthManager
from grpc_client import GRPCClient
from message_handler import MessageHandler
from utils import initialize_session, setup_logging

# Configuração do logging
logger = setup_logging()
logger.info("Aplicativo Chat Empresas iniciado.")

# Constantes
USER_AVATAR = "🧑‍⚕️"
BOT_AVATAR = "🤖"


def main():
    """Função principal que executa a aplicação Chat Empresas.

    Configura a interface de usuário, gerencia o fluxo de autenticação e interação no chat.
    """
    # Inicialização da sessão
    initialize_session()

    # Títulos
    st.title("Chat Empresas -  Onde podemos conversar sobre o Cadastro das empresas Brasileiras")
    st.sidebar.title("Chat X")
    st.sidebar.markdown("---")
    st.sidebar.header("Você é novo por aqui?")

    # Instâncias das classes
    auth_manager = AuthManager()
    grpc_client = GRPCClient()

    # Opções de autenticação
    auth_option = st.sidebar.radio(
        "Opção de Login:",
        ["Use seu e-mail de registro", "Novo Usuário"]
    )

    # Fluxo de autenticação
    if not st.session_state.is_logged_in:
        if auth_option == "Novo Usuário":
            user_email_input = st.sidebar.text_input("Digite seu e-mail")
            if st.sidebar.button("Registrar"):
                user_email = user_email_input.strip()
                if user_email:
                    logger.info(f"Solicitação de registro para o e-mail: {user_email}")
                    success = auth_manager.register_user(email=user_email)
                    if success:
                        st.sidebar.success("Registro bem-sucedido!")
                        st.session_state.is_logged_in = True
                        st.session_state.useremail = user_email
                        logger.info(f"Usuário {user_email} registrado e logado com sucesso.")
                    else:
                        st.sidebar.error("Falha no registro. O e-mail já está registrado?")
                        logger.warning(f"Falha no registro para o e-mail: {user_email}")
                else:
                    st.sidebar.error("Por favor, insira um e-mail válido.")
                    logger.warning("Tentativa de registro com e-mail vazio.")
        else:
            user_email_input = st.sidebar.text_input("Digite seu e-mail")
            if st.sidebar.button("Login"):
                user_email = user_email_input.strip()
                if user_email:
                    logger.info(f"Solicitação de login para o e-mail: {user_email}")
                    if auth_manager.login_user(email=user_email):
                        st.sidebar.success("Login bem-sucedido!")
                        st.session_state.is_logged_in = True
                        st.session_state.useremail = user_email
                        logger.info(f"Usuário {user_email} autenticado e logado com sucesso.")
                    else:
                        st.sidebar.error("Credenciais inválidas. Tente novamente.")
                        logger.warning(f"Falha na autenticação para o e-mail: {user_email}")
                else:
                    st.sidebar.error("Por favor, insira um e-mail válido.")
                    logger.warning("Tentativa de login com e-mail vazio.")

    # Interface do chat
    if st.session_state.is_logged_in:
        st.sidebar.empty()  # Limpa a sidebar após login
        logger.debug(f"Exibindo interface de chat para o usuário {st.session_state.useremail}.")

        # Instância do manipulador de mensagens
        message_handler = MessageHandler(user_email=st.session_state.useremail)

        # Exibir informações do usuário
        st.sidebar.text(f"Usuário: {st.session_state.useremail}")
        st.sidebar.text(f"Sua cota de prompt é: {message_handler.get_message_limit()}")

        # Carregar e exibir mensagens
        messages = message_handler.load_user_messages()
        for message in messages:
            avatar = USER_AVATAR if message["role"] == "user" else BOT_AVATAR
            with st.chat_message(message["role"], avatar=avatar):
                st.markdown(message["content"])

        # Input de novas mensagens
        user_question = st.chat_input("Como posso te ajudar?")
        if user_question:
            logger.info(f"Usuário {st.session_state.useremail} enviou uma pergunta: {user_question}")
            # Salvar e exibir a mensagem do usuário
            message_handler.save_user_message(user_question)
            messages.append({"role": "user", "content": user_question})
            with st.chat_message("user", avatar=USER_AVATAR):
                st.markdown(user_question)

            # Processar e exibir a resposta do assistente
            with st.chat_message("assistant", avatar=BOT_AVATAR):
                message_placeholder = st.empty()
                with st.spinner("Processando..."):
                    try:
                        response = asyncio.run(grpc_client.ask_question(user_question))
                        logger.info(f"Resposta recebida para a pergunta '{user_question}': {response}")
                    except Exception as e:
                        response = "Desculpe, ocorreu um erro ao processar sua pergunta."
                        logger.error(f"Erro ao obter resposta para a pergunta '{user_question}': {e}", exc_info=True)
                message_handler.save_assistant_message(response)
                message_placeholder.markdown(response)
                messages.append({"role": "assistant", "content": response})
                message_handler.update_counter()
    else:
        st.error("Por favor, faça o login para continuar.")
        logger.info("Acesso negado: usuário não autenticado.")


if __name__ == "__main__":
    main()
