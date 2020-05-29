import json
from ETL import run_etl

def auto_etl(event, context):
    body = {
        "message": "upload win!!",
        "input": event
    }

    filename = event['Records'][0]['s3']['object']['key']
    run_etl(filename)

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }



    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """
