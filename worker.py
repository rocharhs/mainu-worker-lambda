import boto3
import json

from twilio.rest import Client

ssm = boto3.client("ssm")

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
            response_text = f"echo: {incoming_text}"

            response_message = client.messages.create(
                body=response_text,
                from_=TWILIO_WHATSAPP_NUMBER,
                to=user_number
            )

            print("Message sent:", response_message.sid)
    else:
        print("Nenhum registro SQS encontrado no evento.")
        
    print("--- WORKER FINALIZADO COM SUCESSO ---")
    
    return {
        "statusCode": 200,
        "body": json.dumps("Hello World do Worker processado!")
    }
