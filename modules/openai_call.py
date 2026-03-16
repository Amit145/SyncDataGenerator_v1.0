from openai import AzureOpenAI
import json
import os
from dotenv import load_dotenv
from anthropic import AnthropicVertex

# from modules.functions import log_message
load_dotenv()

# Keep global client initialization -gpt4o
openai_global_client = AzureOpenAI(
    api_key=os.getenv('API_KEY'),
    api_version=os.getenv('OPENAI_API_VERSION'),
    azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
)

# Gpt5-chat
client = AzureOpenAI(api_version=os.getenv('API_VERSION'), azure_endpoint=os.getenv('ENDPOINT'),
                     api_key=os.getenv('SUBSCRIPTION_KEY'), )

# GPT-5
gpt5_client = AzureOpenAI(
    api_key=os.getenv('AZURE_OPENAI_API_KEY_GPT_5o'),
    api_version=os.getenv('OPENAI_API_VERSION_GPT_5o'),
    azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT_GPT_5o'),
)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './claude_testing/GCP-Service-Account-Key.json'
exl_engine = os.getenv('ENGINE')


def convert_with_llm(prompt_llm, model_name='openai'):
    if model_name == 'openai':
        print(f"Calling LLM with model OpenAI", "INFO")
        try:
            # Use the global client directly or create a local one if preferred
            completion = openai_global_client.chat.completions.create(
                model=exl_engine,
                messages=[{"role": "user", "content": prompt_llm}],
                temperature=0,
            )
            completion_json = json.loads(completion.model_dump_json(indent=2))
            response = completion_json['choices'][0]['message']['content']
        except Exception as e:
            response = 'NA'
            completion_json = str(e)
            print(f"OpenAI Error: {e}")
        return response, completion_json

    elif model_name == 'claude':
        print(f"Calling LLM with model Claude", "INFO")
        try:
            # Initialize Anthropic client locally within this block
            anthropic_client_local = AnthropicVertex(
                region="us-east5",  # or your preferred region
                project_id="anl-cv-code-harbor-gcp"
            )

            message = anthropic_client_local.messages.create(
                max_tokens=8000,
                messages=[
                    {
                        "role": "user",
                        "content": prompt_llm,
                    }
                ],
                model="claude-sonnet-4@20250514"
            )
            # print( message.content[0].text)
            return message.content[0].text, message
        except Exception as e:
            response = 'NA'
            completion_json = str(e)
            print(f"Claude Error: {e}")
        return response, completion_json
    elif model_name == 'gpt-5-chat':
        print(f"Calling LLM with model GPT-5-Chat", "INFO")
        try:
            completion = client.chat.completions.create(
                model="gpt-5-chat",
                messages=[{"role": "user", "content": prompt_llm}],
                temperature=0,
            )
            completion_json = json.loads(completion.model_dump_json(indent=2))
            response = completion_json['choices'][0]['message']['content']
        except Exception as e:
            response = 'NA'
            completion_json = str(e)
            print(f"GPT-5-Chat Error: {e}")
        return response, completion_json

    elif model_name == 'gpt-5':
        print(f"Calling LLM with model GPT-5", "INFO")
        try:
            completion = gpt5_client.chat.completions.create(
                model="gpt-5",
                messages=[{"role": "user", "content": prompt_llm}],
                temperature=0,
            )
            completion_json = json.loads(completion.model_dump_json(indent=2))
            response = completion_json['choices'][0]['message']['content']
        except Exception as e:
            response = 'NA'
            completion_json = str(e)
            print(f"GPT-5o Error: {e}")
        return response, completion_json

    else:
        return "Unsupported model", None
