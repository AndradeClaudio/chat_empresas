import sqlite3
from contextlib import contextmanager
from typing import List, Optional, Tuple, Dict
import streamlit as st
import pandas as pd
import logging
import os

# Constantes
DATABASE_FILE = "database.db"
LOG_FILE = "logs/autheticate.log"
MESSAGE_LIMIT = 20  # Defina o limite de mensagens aqui

# Configuração do Logging
def setup_logging(log_file: str = LOG_FILE):
    """Configura o sistema de logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

# Inicializa o sistema de logging
setup_logging()
logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Exceção personalizada para erros de banco de dados."""
    pass


class DatabaseManager:
    """Gerencia as operações de banco de dados SQLite."""

    def __init__(self, db_file: str = DATABASE_FILE):
        self.db_file = db_file
        self.logger = logging.getLogger(self.__class__.__name__)
        self.initialize_database()

    @contextmanager
    def get_connection(self):
        """Context manager para conexões com o banco de dados."""
        try:
            conn = sqlite3.connect(self.db_file)
            yield conn
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise DatabaseError(f"Erro no banco de dados: {e}")
        finally:
            conn.close()

    def initialize_database(self):
        """Cria as tabelas necessárias no banco de dados SQLite."""
        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS users (
                useremail TEXT PRIMARY KEY
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS thread_save (
                useremail TEXT PRIMARY KEY,
                thread_key TEXT,
                FOREIGN KEY (useremail) REFERENCES users(useremail)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS message_limit (
                useremail TEXT PRIMARY KEY,
                counter INTEGER DEFAULT 0,
                FOREIGN KEY (useremail) REFERENCES users(useremail)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                useremail TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (useremail) REFERENCES users(useremail)
            )
            """
        ]

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for query in create_table_queries:
                    cursor.execute(query)
                conn.commit()
            self.logger.info("Banco de dados inicializado com sucesso.")
        except DatabaseError as e:
            self.logger.error(f"Falha ao inicializar o banco de dados: {e}")
            st.error("Erro ao inicializar o banco de dados.")

    # Métodos de Usuário
    def add_user(self, useremail: str) -> bool:
        """Adiciona um novo usuário ao banco de dados."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO users (useremail) VALUES (?)", (useremail,))
                conn.commit()
            self.logger.info(f"Usuário registrado: {useremail}")
            return True
        except sqlite3.IntegrityError:
            # Usuário já existe
            self.logger.warning(f"Tentativa de registrar usuário existente: {useremail}")
            return False
        except DatabaseError as e:
            self.logger.error(f"Erro ao registrar usuário {useremail}: {e}")
            st.error("Erro ao registrar usuário.")
            return False

    def user_exists(self, useremail: str) -> bool:
        """Verifica se um usuário existe no banco de dados."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM users WHERE useremail = ?", (useremail,))
                exists = cursor.fetchone() is not None
            self.logger.debug(f"Verificação de existência do usuário {useremail}: {exists}")
            return exists
        except DatabaseError as e:
            self.logger.error(f"Erro ao verificar existência do usuário {useremail}: {e}")
            return False

    # Métodos de Thread
    def get_thread_key(self, useremail: str) -> Optional[str]:
        """Obtém a chave da thread para um usuário específico."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT thread_key FROM thread_save WHERE useremail = ?", (useremail,))
                result = cursor.fetchone()
            thread_key = result[0] if result else None
            self.logger.debug(f"Chave de thread para {useremail}: {thread_key}")
            return thread_key
        except DatabaseError as e:
            self.logger.error(f"Erro ao obter chave de thread para {useremail}: {e}")
            return None

    def set_thread_key(self, useremail: str, thread_key: str) -> bool:
        """Define ou atualiza a chave da thread para um usuário."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO thread_save (useremail, thread_key)
                    VALUES (?, ?)
                    ON CONFLICT(useremail) DO UPDATE SET thread_key=excluded.thread_key
                """, (useremail, thread_key))
                conn.commit()
            self.logger.info(f"Chave de thread atualizada para {useremail}.")
            return True
        except DatabaseError as e:
            self.logger.error(f"Erro ao definir chave de thread para {useremail}: {e}")
            st.error("Erro ao definir chave da thread.")
            return False

    # Métodos de Limite de Mensagens
    def get_message_limit(self, useremail: str) -> Tuple[bool, int]:
        """Obtém o status do limite de mensagens para um usuário."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT counter FROM message_limit WHERE useremail = ?", (useremail,))
                result = cursor.fetchone()

            if result:
                is_below_limit = result[0] < MESSAGE_LIMIT
                self.logger.debug(f"Usuário {useremail} enviou {result[0]} mensagens.")
                return is_below_limit, result[0]
            else:
                self.logger.debug(f"Limite de mensagens não inicializado para {useremail}.")
                return False, 0
        except DatabaseError as e:
            self.logger.error(f"Erro ao obter limite de mensagens para {useremail}: {e}")
            return False, 0

    def initialize_message_limit(self, useremail: str) -> bool:
        """Inicializa o contador de mensagens para um novo usuário."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO message_limit (useremail, counter) VALUES (?, 0)", (useremail,))
                conn.commit()
            self.logger.info(f"Limite de mensagens inicializado para {useremail}.")
            return True
        except sqlite3.IntegrityError:
            # Limite de mensagens já inicializado
            self.logger.warning(f"Tentativa de inicializar limite de mensagens existente para {useremail}.")
            return False
        except DatabaseError as e:
            self.logger.error(f"Erro ao inicializar limite de mensagens para {useremail}: {e}")
            st.error("Erro ao inicializar limite de mensagens.")
            return False

    def update_message_counter(self, useremail: str, increment: int = 1) -> Optional[int]:
        """Atualiza o contador de mensagens para um usuário."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE message_limit
                    SET counter = counter + ?
                    WHERE useremail = ?
                """, (increment, useremail))
                conn.commit()

                cursor.execute("SELECT counter FROM message_limit WHERE useremail = ?", (useremail,))
                result = cursor.fetchone()
            new_counter = result[0] if result else None
            self.logger.info(f"Contador de mensagens atualizado para {useremail}: {new_counter}")
            return new_counter
        except DatabaseError as e:
            self.logger.error(f"Erro ao atualizar contador de mensagens para {useremail}: {e}")
            st.error("Erro ao atualizar contador de mensagens.")
            return None

    # Métodos de Mensagens
    def save_message(self, useremail: str, role: str, content: str) -> bool:
        """Salva uma mensagem no banco de dados."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO messages (useremail, role, content)
                    VALUES (?, ?, ?)
                """, (useremail, role, content))
                conn.commit()
            self.logger.info(f"Mensagem salva para {useremail}: {role} - {content[:50]}...")
            return True
        except DatabaseError as e:
            self.logger.error(f"Erro ao salvar mensagem para {useremail}: {e}")
            st.error("Erro ao salvar mensagem.")
            return False

    def load_messages(self, useremail: str) -> List[Dict[str, str]]:
        """Carrega todas as mensagens de um usuário."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT role, content FROM messages
                    WHERE useremail = ?
                    ORDER BY timestamp ASC
                """, (useremail,))
                messages = cursor.fetchall()
            self.logger.debug(f"{len(messages)} mensagens carregadas para {useremail}.")
            return [{"role": role, "content": content} for role, content in messages]
        except DatabaseError as e:
            self.logger.error(f"Erro ao carregar mensagens para {useremail}: {e}")
            return []


class UserAuthenticator:
    """Gerencia a autenticação de usuários."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(self.__class__.__name__)

    def authenticate_user(self, useremail: str) -> bool:
        """Verifica se o usuário está autenticado."""
        exists = self.db_manager.user_exists(useremail)
        self.logger.info(f"Autenticação para {useremail}: {'sucesso' if exists else 'falha'}")
        return exists

    def register_user(self, useremail: str) -> bool:
        """Registra um novo usuário."""
        success = self.db_manager.add_user(useremail)
        if success:
            self.db_manager.initialize_message_limit(useremail)
            self.logger.info(f"Usuário registrado e limite de mensagens inicializado: {useremail}")
        else:
            self.logger.warning(f"Registro de usuário falhou ou usuário já existe: {useremail}")
        return success


class MessageService:
    """Serviço para gerenciar mensagens dos usuários."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(self.__class__.__name__)

    def save_message(self, useremail: str, role: str, content: str) -> bool:
        """Salva uma mensagem e atualiza o contador."""
        if self.db_manager.save_message(useremail, role, content):
            new_count = self.db_manager.update_message_counter(useremail)
            if new_count is not None and new_count <= MESSAGE_LIMIT:
                self.logger.info(f"Mensagem enviada por {useremail}. Contador: {new_count}/{MESSAGE_LIMIT}")
                return True
            else:
                self.logger.warning(f"Usuário {useremail} atingiu o limite de mensagens.")
        return False

    def load_messages(self, useremail: str) -> List[Dict[str, str]]:
        """Carrega as mensagens do usuário."""
        messages = self.db_manager.load_messages(useremail)
        self.logger.debug(f"Mensagens carregadas para {useremail}: {len(messages)}")
        return messages


# Inicialização dos componentes
db_manager = DatabaseManager()
authenticator = UserAuthenticator(db_manager)
message_service = MessageService(db_manager)


# Interface do Streamlit
def main():
    st.title("Aplicação Streamlit com SQLite e Logging")

    menu = ["Login", "Registro"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Registro":
        st.subheader("Crie uma nova conta")
        useremail = st.text_input("Email")
        if st.button("Registrar"):
            if authenticator.register_user(useremail):
                st.success("Usuário registrado com sucesso!")
                logger.info(f"Usuário registrado via interface: {useremail}")
            else:
                st.error("Usuário já existe ou ocorreu um erro.")
                logger.warning(f"Falha no registro via interface para: {useremail}")

    elif choice == "Login":
        st.subheader("Faça login na sua conta")
        useremail = st.text_input("Email")
        if st.button("Login"):
            if authenticator.authenticate_user(useremail):
                st.success("Autenticado com sucesso!")
                logger.info(f"Usuário autenticado via interface: {useremail}")
                user_session(useremail)
            else:
                st.error("Usuário não encontrado. Por favor, registre-se.")
                logger.warning(f"Falha na autenticação via interface para: {useremail}")


def user_session(useremail: str):
    st.write(f"Bem-vindo, {useremail}!")

    # Gerenciar limites de mensagens
    is_allowed, count = db_manager.get_message_limit(useremail)
    st.write(f"Mensagens enviadas: {count}/{MESSAGE_LIMIT}")

    if not is_allowed:
        st.warning("Limite de mensagens atingido.")
        logger.info(f"Usuário atingiu o limite de mensagens: {useremail}")
        return

    # Interface para enviar mensagens
    role = st.selectbox("Role", ["user", "assistant"])
    content = st.text_area("Mensagem")
    if st.button("Enviar"):
        if message_service.save_message(useremail, role, content):
            st.success("Mensagem enviada com sucesso!")
            logger.info(f"Mensagem enviada por {useremail}: {content[:50]}...")
            st.experimental_rerun()
        else:
            st.error("Erro ao enviar mensagem.")
            logger.error(f"Falha ao enviar mensagem para {useremail}: {content[:50]}...")

    # Exibir mensagens
    messages = message_service.load_messages(useremail)
    if messages:
        st.subheader("Histórico de Mensagens")
        for msg in messages:
            st.write(f"**{msg['role'].capitalize()}**: {msg['content']}")


if __name__ == "__main__":
    main()
