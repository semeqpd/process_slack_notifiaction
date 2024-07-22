import os
import json
import boto3
import logging
import tempfile
import requests
from datetime import datetime
from botocore.exceptions import ClientError

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/teste")
BUCKET = os.environ.get("BUCKET", "process-error")


def lambda_handler(event, context):
    for record in event["Records"]:
        queue_arn = record["eventSourceARN"]
        queue_name = queue_arn.split(":")[-1]
        
        try:
            body = json.loads(record["body"].replace("'", '"'))
        except json.JSONDecodeError as e:
            logging.error(f"Erro ao decodificar o JSON: {e}")
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Erro ao decodificar o JSON",
                    "error": str(e)
                })
            }

    plt = body.get("plt", "unknown")
    smq = body.get("smq", "")

    smq_parts = smq.split(";")
    
    i = None
    h = None

    try:
        for part in smq_parts:
            if part.startswith("i:"):
                i = part.split(":")[1]
            elif part.startswith("h:"):
                h = part.split(":")[1]
        
        if not i or not h:
            raise ValueError("Campos 'i' ou 'h' não encontrados ou inválidos no SMQ.")

        h_datetime = datetime.strptime(h, "%d%m%Y%H%M%S")
        h_formatted = h_datetime.strftime("%d/%m/%Y %H:%M:%S")
        path_formatted = h_datetime.strftime("%Y%m%d%H%M%S")
        
        object_name = f"{plt}/{path_formatted}/{i}.json"
    except (IndexError, ValueError) as e:
        logging.error(f"Erro ao processar o SMQ: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Erro ao processar o SMQ",
                "error": str(e)
            })
        }

    print(queue_name)
    print(plt)
    print(i)
    print(h)
    print(path_formatted)
    print(object_name)

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(json.dumps(body).encode())
    temp_file_path = temp_file.name
    temp_file.close()

    s3_client = boto3.client("s3")

    try:
        if queue_name == "process_st_dlq_queue":
            print("Simulando envio para S3 sem realmente enviar.")
            print("Objeto seria inserido no S3")
        elif queue_name == "process_dlq_queue":
            s3_client.upload_file(temp_file_path, BUCKET, object_name)
            print("Objeto inserido com sucesso no S3")
        else:
            logging.warning(
                f"Fila não reconhecida: {queue_name}. Objeto não enviado para o S3."
            )
    except ClientError as e:
        logging.error(f"Erro ao enviar objeto para o S3: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Erro ao enviar objeto para o S3",
                "error": str(e)
            })
        }
    finally:
        os.remove(temp_file_path)

    try:
        slack_message = {
            "text": f"*NOVA MENSAGEM ENVIADA PARA UMA FILA DE DLQ:*\n"
                    f"*Fila*: {queue_name}\n"
                    f"*Planta*: {plt}\n"
                    f"*Sensor*: {i}\n"
                    f"*Data*: {h_formatted}\n"
                    f"*Path do S3*: {object_name}"
        }
        if queue_name == "process_st_dlq_queue":
            print("Simulando envio de mensagem para o Slack.")
            print(f"Mensagem Slack: {json.dumps(slack_message, indent=2)}")
        else:
            response = requests.post(SLACK_WEBHOOK_URL, json=slack_message)
            response.raise_for_status()
            print("Mensagem enviada com sucesso para o Slack!")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao enviar mensagem para o Slack: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Erro ao enviar mensagem para o Slack",
                "error": str(e)
            })
        }

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Processamento concluído com sucesso"
        })
    }