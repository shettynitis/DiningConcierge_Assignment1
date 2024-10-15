import boto3
import uuid

client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    # Extract the message from the API request
    message = event['messages'][0]['unstructured']['text']
    # Generate a unique session ID
    session_id = str(uuid.uuid4())

    # Call Lex bot
    response = client.recognize_text(
        botId='JUJPLO17HL',
        botAliasId='TSTALIASID',
        localeId='en_US',
        sessionId= 'session_1', # Change session id later send from front end
        text=message
    )
    
    # Extract Lex's response
    lex_message = response['messages'][0]['content']

    # Return the response to the frontend
    return {
        'statusCode': 200,
        'messages': [
            {
                "type": "unstructured",
                "unstructured": {
                    "text": lex_message
                }
            }
        ]
    }