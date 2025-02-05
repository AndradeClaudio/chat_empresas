import asyncio
import grpc
from grpc import aio
import genai_pb2
import genai_pb2_grpc
import time  # Importando o módulo time para medir o tempo

async def main():
    start_time = time.time()  # Marca o início do tempo
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = genai_pb2_grpc.GenAiServiceStub(channel)
        
        user_question = "Quantas empresas ativa exitem?"
        request = genai_pb2.QuestionRequest(question=user_question)
        # Faz chamada assíncrona ao servidor
        response = stub.AskQuestion(request)
        end_time = time.time()  # Marca o tempo de resposta
        print("Resposta do servidor:", response.answer)
        print(f"Tempo de resposta: {end_time - start_time} segundos")  # Exibe o tempo de resposta

if __name__ == "__main__":
    asyncio.run(main())
