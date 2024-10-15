import requests
from requests.auth import HTTPBasicAuth
import json
import random
import boto3

# AWS Resources
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')

# Configuration
queue_url = 'https://sqs.us-east-1.amazonaws.com/423623871680/DiningSuggestionsQueue'
table = dynamodb.Table('yelp-restaurants') 
region = 'us-east-1'

# OpenSearch domain details
master_user = 'NitishaShetty'  # OpenSearch Master User
master_password = 'Password@98'  # OpenSearch Master Password
opensearch_url = 'https://search-dining-domain-xumyoad3xjnwxcyh33k2aveezm.us-east-1.es.amazonaws.com'

# OpenSearch index and document type
index_name = "restaurants"

opensearch_url = f'{opensearch_url}/{index_name}/_search'

headers = { "Content-Type": "application/json" }

# Helper Function: Get message from SQS
def get_message_from_sqs():
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )
    if 'Messages' in response:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        body = json.loads(message['Body'])
        return body, receipt_handle
    return None, None

# Helper Function: Get random restaurant recommendation from OpenSearch (Elasticsearch)
def search_restaurant(cuisine):
    query = {
        "size": 5,
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        }
    }
    response = requests.post(opensearch_url, auth=HTTPBasicAuth(master_user, master_password), headers=headers, data=json.dumps(query))
    
    result = response.json()
    print(response)
    print(result)
    if 'hits' in result and result['hits']['hits']:
        random_choice = random.choice(result['hits']['hits'])
        return random_choice['_source']['RestaurantID']
    
    return None

# Helper Function: Get detailed restaurant information from DynamoDB
def get_restaurant_details(restaurant_id):
    response = table.get_item(Key={'BusinessID': restaurant_id})
    if 'Item' in response:
        return response['Item']
    return None

# Helper Function: Format recommendation message
def format_recommendation(restaurant):
    if restaurant:
        return f"Restaurant Name: {restaurant['Name']}\nAddress: {restaurant['Address']}\nRating: {restaurant['Rating']}\n"
    return "No recommendations found."

# Helper Function: Send email using Amazon SES
def send_email(to_address, subject, body):
    ses.send_email(
        Source='nitisha.shetty@gmail.com',
        Destination={'ToAddresses': [to_address]},
        Message={
            'Subject': {'Data': subject},
            'Body': {'Text': {'Data': body}}
        }
    )

# Main Lambda Handler
def lambda_handler(event, context):
    # Step 1: Pull message from SQS
    message, receipt_handle = get_message_from_sqs()
    
    if not message:
        return {"statusCode": 200, "body": "No messages in queue"}
    
    # Extract data from SQS message
    cuisine = message['Cuisine']
    email = message['Email']

    # Step 2: Get random restaurant from OpenSearch
    restaurant_id = search_restaurant(cuisine)

    if not restaurant_id:
        send_email(email, "No Recommendation", f"Sorry, we couldn't find any recommendations for {cuisine}.")
        return {"statusCode": 200, "body": "No restaurant found."}

    # Step 3: Get restaurant details from DynamoDB
    restaurant = get_restaurant_details(restaurant_id)

    # Step 4: Format the recommendation
    recommendation = format_recommendation(restaurant)

    # Step 5: Send email with the recommendation
    send_email(email, f"Recommendation for {cuisine}", recommendation)

    # Delete the message from the SQS queue after processing
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    return {"statusCode": 200, "body": "Recommendation sent successfully."}

