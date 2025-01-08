from dotenv import load_dotenv
from langchain_aws import ChatBedrockConverse
import os

load_dotenv()

def main():
    # Initialize the ChatBedrockConverse client
    chat_client = ChatBedrockConverse(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                      region_name=os.getenv('AWS_REGION', 'us-east-1'),
                                        model_id="mistral.mistral-7b-instruct-v0:2",
                                        temperature=0,
                                        max_tokens=512,)
    
    # Example conversation
    user_input = "Hello, how can I help you today?"
    
    messages = [
        (
            "system",
            "You are a helpful assistant that translates English to French. Translate the user sentence.",
        ),
        ("human", "I love programming."),
    ]
    ai_msg = chat_client.invoke(messages)
    #ai_msg
    #print("User: ", user_input)
    print("Bot: ", ai_msg)

if __name__ == "__main__":
    main()
