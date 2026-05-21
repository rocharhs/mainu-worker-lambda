import json

def handler(event, context):
    print("--- WORKER INICIADO e atualizado via CI/CD---")
    print(f"Evento recebido do SQS: {json.dumps(event)}")
    
    # Verifica se o evento veio mesmo do SQS e tem registros
    if 'Records' in event:
        print(f"Total de mensagens recebidas neste lote: {len(event['Records'])}")
        
        for record in event['Records']:
            message_id = record.get('messageId')
            body = record.get('body')
            print(f"Processando Mensagem ID: {message_id}")
            print(f"Conteúdo do Body: {body}")
    else:
        print("Nenhum registro SQS encontrado no evento.")
        
    print("--- WORKER FINALIZADO COM SUCESSO ---")
    
    return {
        "statusCode": 200,
        "body": json.dumps("Hello World do Worker processado!")
    }
