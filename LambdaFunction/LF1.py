import json
import boto3

# Initialize SQS client
sqs = boto3.client('sqs')

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')

# Reference the table
table = dynamodb.Table('UserState')

def save_user_state(email, location, cuisine, dining_time, number_of_people):
    # Prepare the item to be saved
    item = {
        'EmailID': email,  # Partition Key
        'LastLocation': location,
        'LastCuisine': cuisine,
        'LastDiningTime': dining_time,
        'LastNumberOfPeople': number_of_people
    }
    
    # Put the item into the table (this will replace the item if it already exists)
    table.put_item(Item=item)

    print(f"Saved state for {email}")
    
def get_previous_state(email):
    try:
        response = table.get_item(Key={'EmailID': email})
        return response.get('Item', None)
    except Exception as e:
        print(f"Error retrieving previous state for {email}: {str(e)}")
        return None
        
        
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
    elif intent_name == 'PreviousSearchIntent':
        return previous_search_intent_response(event)
    else:
        return fallback_intent_response()
        
def previous_search_intent_response(event):
    # Extract email from slots
    slots = event['sessionState']['intent']['slots']
    email = slots['Email']['value']['interpretedValue']

    # Get the previous search state from DynamoDB
    previous_state = get_previous_state(email)
    
    if previous_state:
        # If previous state exists, return it
        location = previous_state['LastLocation']
        cuisine = previous_state['LastCuisine']
        dining_time = previous_state['LastDiningTime']
        number_of_people = previous_state['LastNumberOfPeople']
        
        queue_url = 'https://sqs.us-east-1.amazonaws.com/423623871680/DiningSuggestionsQueue'
        message = {
            'Location': location,
            'Cuisine': cuisine,
            'Dining_Time': dining_time,
            'Number_of_People': number_of_people,
            'Email': email
        }
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
    
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Close'
                },
                'intent': {
                    'name': 'PreviousSearchIntent',
                    'state': 'Fulfilled'
                }
            },
            'messages': [
                {
                    'contentType': 'PlainText',
                    'content': f"Your last search was for {number_of_people} people to dine in {location} at {dining_time} with {cuisine}. We will email you the suggestions soon."
                }
            ]
        }
    else:
        # No previous state found
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Close'
                },
                'intent': {
                    'name': 'PreviousSearchIntent',
                    'state': 'Fulfilled'
                }
            },
            'messages': [
                {
                    'contentType': 'PlainText',
                    'content': 'No previous search found for this email. Please start a new search.'
                }
            ]
        }


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
    save_user_state(email, location, cuisine, dining_time, number_of_people)
    
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
                'content': f'Great! I\'ve noted your preferences for {number_of_people} people to dine in {location} at {dining_time}, with {cuisine}. You will receive restaurant suggestions at {email} shortly.\n Have a good day!'
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
