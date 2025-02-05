# auth.py

import logging
import re
from authenticate import UserAuthenticator, DatabaseManager

class AuthManager:
    """Gerencia a autenticação de usuários no aplicativo Chat X.

    Esta classe fornece métodos para registrar novos usuários e autenticar usuários existentes.
    """

    def __init__(self):
        """Inicializa o AuthManager.

        Configura os atributos de email e chave de thread, além do logger.
        """
        self.user_email = None
        self.thread_key = None
        self.logger = logging.getLogger(__name__)
        self.logger.debug("AuthManager inicializado.")
        self.db_manager = DatabaseManager()
        self.authenticator = UserAuthenticator(self.db_manager)

    def is_valid_email(self, email: str) -> bool:
        """Valida o formato do e-mail.

        Args:
            email (str): O e-mail a ser validado.

        Returns:
            bool: True se o e-mail for válido, False caso contrário.
        """
        email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        return re.match(email_regex, email) is not None

    def register_user(self, email: str) -> bool:
        """Registra um novo usuário no sistema.

        Verifica se o usuário já está registrado. Se não estiver, insere os dados do usuário
        e define o limite inicial de mensagens.

        Args:
            email (str): O e-mail do usuário a ser registrado.

        Returns:
            bool: True se o registro for bem-sucedido, False caso contrário.
        """
        if not self.is_valid_email(email):
            self.logger.warning(f"E-mail inválido: {email}")
            return False

        self.logger.info(f"Tentando registrar o usuário: {email}")
        try:
            if self.authenticator.register_user(email):
                self.user_email = email
                self.logger.info(f"Usuário {email} registrado com sucesso.")
                return True
            else:
                self.logger.warning(f"Usuário {email} já está registrado ou ocorreu um erro.")
            return False
        except Exception as e:
            self.logger.error(f"Erro ao registrar o usuário {email}: {e}", exc_info=True)
            return False

    def login_user(self, email: str) -> bool:
        """Autentica um usuário existente no sistema.

        Verifica as credenciais do usuário e, se válidas, recupera a chave da thread.

        Args:
            email (str): O e-mail do usuário a ser autenticado.

        Returns:
            bool: True se a autenticação for bem-sucedida, False caso contrário.
        """
        if not self.is_valid_email(email):
            self.logger.warning(f"E-mail inválido: {email}")
            return False

        self.logger.info(f"Tentando autenticar o usuário: {email}")
        try:
            if self.authenticator.authenticate_user(email):
                self.user_email = email
                self.thread_key = self.db_manager.get_thread_key(email)
                self.logger.info(f"Usuário {email} autenticado com sucesso.")
                return True
            self.logger.warning(f"Autenticação falhou para o usuário {email}.")
            return False
        except Exception as e:
            self.logger.error(f"Erro ao autenticar o usuário {email}: {e}", exc_info=True)
            return False
