# message_handler.py

import logging
from authenticate import DatabaseManager, MessageService

MESSAGE_LIMIT = 1000  # Defina o limite de mensagens aqui

class MessageHandler:
    """Gerencia o armazenamento e recuperação de mensagens dos usuários.

    Esta classe fornece métodos para salvar mensagens do usuário e do assistente,
    carregar mensagens anteriores e gerenciar os limites de mensagens.
    """

    def __init__(self, user_email: str):
        """Inicializa o MessageHandler para um usuário específico.

        Configura o email do usuário e inicializa o logger.

        Args:
            user_email (str): O e-mail do usuário.
        """
        self.user_email = user_email
        self.logger = logging.getLogger(__name__)
        self.logger.debug(f"MessageHandler inicializado para o usuário {self.user_email}.")
        self.db_manager = DatabaseManager()
        self.message_service = MessageService(self.db_manager)

    def get_message_limit(self) -> int:
        """Obtém o número de mensagens restantes que o usuário pode enviar.

        Calcula a diferença entre o limite total e o número de mensagens já enviadas.

        Returns:
            int: Número de mensagens restantes.
        """
        self.logger.debug(f"Obtendo limite de mensagens para {self.user_email}.")
        try:
            is_below_limit, value = self.db_manager.get_message_limit(self.user_email)
            remaining = MESSAGE_LIMIT - value
            self.logger.info(f"{self.user_email} tem {remaining} mensagens restantes.")
            return remaining
        except Exception as e:
            self.logger.error(f"Erro ao obter limite de mensagens para {self.user_email}: {e}", exc_info=True)
            return 0

    def save_user_message(self, message: str):
        """Salva uma mensagem enviada pelo usuário.

        Registra a mensagem no armazenamento de dados.

        Args:
            message (str): A mensagem do usuário a ser salva.
        """
        self.logger.debug(f"Salvando mensagem do usuário {self.user_email}: {message}")
        try:
            self.message_service.save_message(self.user_email, "user", message)
        except Exception as e:
            self.logger.error(f"Erro ao salvar mensagem do usuário {self.user_email}: {e}", exc_info=True)

    def save_assistant_message(self, message: str):
        """Salva uma mensagem enviada pelo assistente.

        Registra a mensagem no armazenamento de dados.

        Args:
            message (str): A mensagem do assistente a ser salva.
        """
        self.logger.debug(f"Salvando mensagem do assistente para {self.user_email}: {message}")
        try:
            self.message_service.save_message(self.user_email, "assistant", message)
        except Exception as e:
            self.logger.error(f"Erro ao salvar mensagem do assistente para {self.user_email}: {e}", exc_info=True)

    def update_counter(self):
        """Atualiza o contador de mensagens enviadas pelo usuário.

        Incrementa o número de mensagens enviadas no armazenamento de dados.
        """
        self.logger.debug(f"Atualizando contador de mensagens para {self.user_email}.")
        try:
            self.db_manager.update_message_counter(self.user_email)
            self.logger.info(f"Contador de mensagens atualizado para {self.user_email}")
        except Exception as e:
            self.logger.error(f"Erro ao atualizar contador de mensagens para {self.user_email}: {e}", exc_info=True)

    def load_user_messages(self) -> list:
        """Carrega todas as mensagens anteriores do usuário.

        Recupera as mensagens armazenadas para o usuário específico.

        Returns:
            list: Lista de dicionários contendo as mensagens do usuário e do assistente.
        """
        self.logger.debug(f"Carregando mensagens para {self.user_email}.")
        try:
            messages = self.message_service.load_messages(self.user_email)
            self.logger.info(f"{len(messages)} mensagens carregadas para {self.user_email}.")
            return messages
        except Exception as e:
            self.logger.error(f"Erro ao carregar mensagens para {self.user_email}: {e}", exc_info=True)
            return []
