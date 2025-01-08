from langchain_aws import ChatBedrock
import os
model_id = "mistral.mistral-7b-instruct-v0:2"
model_kwargs = {
    "max_tokens": 512,
    "temperature": 0,
    "top_k" : 50,
    "top_p": 0.9
}
llm = ChatBedrock(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                      region_name=os.getenv('AWS_REGION', 'us-east-1'),
                                        model_id=model_id,
                                        model_kwargs=model_kwargs,)
messages = [
    (
        "system",
        "You are a helpful assistant that translates English to Portuguese. Translate the user sentence.",
    ),
    ("human", "I love programming."),
]
ai_msg = llm.invoke(messages)
ai_msg
print(ai_msg.content)