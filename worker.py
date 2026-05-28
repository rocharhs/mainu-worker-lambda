import json
import logging
import boto3

from twilio.rest import Client

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")

TWILIO_AUTH_TOKEN = ssm.get_parameter(
    Name="/mainu/twilio/auth",
    WithDecryption=True
)["Parameter"]["Value"]

TWILIO_ACCOUNT_SID = ssm.get_parameter(
    Name="/mainu/twilio/account_sid",
    WithDecryption=True
)["Parameter"]["Value"]


TWILIO_WHATSAPP_NUMBER= ssm.get_parameter(
    Name="/mainu/twilio/number",
    WithDecryption=True
)["Parameter"]["Value"]

LLM_MODEL = "global.anthropic.claude-haiku-4-5-20251001-v1:0"
EMBEDDING_MODEL = "amazon.titan-embed-text-v2:0"

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

def handler(event, context):
    print("--- WORKER INICIADO e atualizado via CI/CD---")
    print(f"Evento recebido do SQS: {json.dumps(event)}")
    
    # Verifica se o evento veio mesmo do SQS e tem registros
    if 'Records' in event:
        print(f"Total de mensagens recebidas neste lote: {len(event['Records'])}")
        
        for record in event['Records']:
            message_id = record.get('messageId')
            body = json.loads(record.get('body'))

            incoming_text = body["content"]
            user_number = body["from"]

            print(f"Processando Mensagem ID: {message_id}")
            print(f"Conteúdo do Body: {body}")

            response = bedrock_client.converse(
                modelId=LLM_MODEL,
                system=[
                    {
                        "text": "Seja sucinto, não use emojis, responda em portugues"
                    }
                ],
                messages=[
                    {
                        "role":"user",
                        "content":[{"text":incoming_text}]
                    }
                ],
                inferenceConfig={
                    "maxTokens": 200,
                    "temperature": 1e-5
                }
            )
            latency = response["metrics"]["latencyMs"]
            input_tokens = response["usage"]["inputTokens"]
            output_tokens = response["usage"]["outputTokens"]
            logger.info(f"took: {latency}ms for {input_tokens} input_tokens/{output_tokens} output_tokens")

            response_text = response["output"]["message"]["content"][0]["text"]

            response_message = client.messages.create(
                body=response_text,
                from_=TWILIO_WHATSAPP_NUMBER,
                to=user_number
            )

            logger.info(f'body:{response_text} from: {TWILIO_WHATSAPP_NUMBER} to: {user_number}')

            print("Message sent:", response_message.sid)
    else:
        print("Nenhum registro SQS encontrado no evento.")
        
    print("--- WORKER FINALIZADO COM SUCESSO ---")
    
    return {
        "statusCode": 200,
        "body": json.dumps("Hello World do Worker processado!")
    }
