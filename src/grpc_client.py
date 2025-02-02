# grpc_client.py

import logging
import asyncio
from grpc import aio
import genai_pb2
import genai_pb2_grpc


class GRPCClient:
    """Cliente para comunicação com o serviço gRPC."""

    def __init__(self, host: str = "localhost", port: int = 50051):
        self.address = f"{host}:{port}"
        self.logger = logging.getLogger(__name__)
        self.logger.debug(f"GRPCClient inicializado com endereço {self.address}.")

    async def ask_question(self, question: str) -> str:
        """Envia uma pergunta ao serviço gRPC e retorna a resposta."""
        self.logger.info(f"Enviando pergunta via gRPC: {question}")
        try:
            async with aio.insecure_channel(self.address) as channel:
                stub = genai_pb2_grpc.GenAiServiceStub(channel)
                request = genai_pb2.QuestionRequest(question=question)
                response = await stub.AskQuestion(request)
                self.logger.debug(f"Recebida resposta do gRPC: {response.answer}")
                return response.answer
        except Exception as e:
            self.logger.error(f"Erro na comunicação gRPC: {e}", exc_info=True)
            return "Desculpe, ocorreu um erro ao processar sua pergunta."
