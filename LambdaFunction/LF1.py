import json
import boto3

# Initialize SQS client
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    print(f"Event received: {json.dumps(event)}")

    # Extract and log the intent name
    intent_name = event['sessionState']['intent']['name']
    print(f"Intent name: {intent_name}")
    if intent_name == 'GreetingIntent':
        return greeting_intent_response()
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent_response()
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions_intent_response(event)
    else:
        return fallback_intent_response()

def greeting_intent_response():
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'GreetingIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': 'Hello! How can I assist you today?'
            }
        ]
    }

def thank_you_intent_response():
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'ThankYouIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': 'You\'re welcome!'
            }
        ]
    }

def dining_suggestions_intent_response(event):
    # Extract slots
    slots = event['sessionState']['intent']['slots']
    location = slots['Location']['value']['interpretedValue']
    cuisine = slots['Cuisine']['value']['interpretedValue']
    dining_time = slots['Dining_Time']['value']['interpretedValue']
    number_of_people = slots['Number_of_People']['value']['interpretedValue']
    email = slots['E_mail']['value']['interpretedValue']

    # Push data to SQS
    queue_url = 'https://sqs.us-east-1.amazonaws.com/423623871680/DiningSuggestionsQueue'
    message = {
        'Location': location,
        'Cuisine': cuisine,
        'Dining_Time': dining_time,
        'Number_of_People': number_of_people,
        'Email': email
    }
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
    
    # Return response to Lex
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'DiningSuggestionsIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': f'Great! I\'ve noted your preferences for {number_of_people} people to dine in {location} at {dining_time}, with {cuisine}. You will receive restaurant suggestions at {email}.'
            }
        ]
    }

def fallback_intent_response():
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'FallbackIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': 'I\'m sorry, I didn\'t understand that. Can you please try again?'
            }
        ]
    }
