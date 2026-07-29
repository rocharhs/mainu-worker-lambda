import json
from datetime import datetime, timezone, timedelta
import logging
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr


logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

LLM_MODEL = "global.anthropic.claude-haiku-4-5-20251001-v1:0"
EMBEDDING_MODEL = "amazon.titan-embed-text-v2:0"

# IDEMPOTENCY_TABLE = ssm.get_parameter(
#     Name="/mainu/twilio/idempotency_table"
# )["Parameter"]["Value"]
# table = dynamodb.Table(IDEMPOTENCY_TABLE)
sessions_table = dynamodb.Table('mainu-sessions')

def generate_answer(incoming_text):
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
    return response_text
    
def handler(event, context):
    print(f"Evento recebido do SQS: {json.dumps(event)}")
    
    # Verifica se o evento veio mesmo do SQS e tem registros
    if 'Records' in event:
        print(f"Total de mensagens recebidas neste lote: {len(event['Records'])}")

        for record in event['Records']:
            body = json.loads(record.get('body'))

            message_id = body['messageId']
            content = body['content']
            session_id = body['sessionId']

            timestamp = datetime.now(timezone.utc).isoformat()
            expires_at = int(
                (datetime.now(timezone.utc) + timedelta(hours=24))
                .timestamp()
            )

            print(f"Processando Mensagem ID: {message_id}")
            print(f"Conteúdo do Body: {body}")

            item = {
                "messageId": message_id,
                "status": "PROCESSING",
                "createdAt": timestamp,
                "expiresAt": expires_at
            }
            # Recupera id de conexão dado id de sessão
            response = sessions_table.get_item(Key={"session_id":session_id})
            session_item = response.get("Item")
            connection_id = session_item.get("connection_id")

            logger.info(item)
            logger.info(type(item["messageId"]))
            logger.info(type(item["status"]))
            logger.info(type(item["createdAt"]))
            logger.info(type(item["expiresAt"]))
            logger.info(content)
            logger.info(f'Connected at {connection_id}')


            # Verifica se mensagem já foi processada
            # try:
            #     table.put_item(
            #         Item=item,
            #         ConditionExpression=Attr("messageSid").not_exists()
            #     )

            #     response_text = generate_answer(incoming_text)

            #     completion_timestamp = datetime.now(timezone.utc).isoformat()

            #     table.update_item(
            #         Key={"messageSid": message_id},
            #         UpdateExpression="SET #s = :s, generatedResponse = :r, completedAt = :t",
            #         ExpressionAttributeNames={
            #             "#s": "status"
            #         },
            #         ExpressionAttributeValues={
            #             ":s": "COMPLETED",
            #             ":r": response_text,
            #             ":t": completion_timestamp
            #         }
            #     )

            #     response_message = client.messages.create(
            #         body=response_text,
            #         from_=TWILIO_WHATSAPP_NUMBER,
            #         to=user_number
            #     )

            #     logger.info(f'body:{response_text} from: {TWILIO_WHATSAPP_NUMBER} to: {user_number}')

            #     print("Message sent:", response_message.sid)

            # except ClientError as e:
            #     if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            #         print("Already being processed or already processed")
            #         return
            #     raise

    else:
        print("Nenhum registro SQS encontrado no evento.")
        
    print("--- WORKER FINALIZADO COM SUCESSO ---")
    
    return {
        "statusCode": 200,
        "body": json.dumps("Hello World do Worker processado!")
    }
