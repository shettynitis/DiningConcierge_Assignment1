def lambda_handler(event, context):
    # Boilerplate response message
    response_message = "I'm still under development. Please come back later."

    return {
        'statusCode': 200,
        'messages': [
            {
                "type": "unstructured",
                "unstructured": {
                    "text": response_message
                }
            }
        ]
    }
