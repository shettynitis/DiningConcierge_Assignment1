import requests
from requests.auth import HTTPBasicAuth
import json
import random
import boto3
import botocore.exceptions

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
opensearch_url = 'https://search-diningdomain-meft6dgd4aul4ho2pyjhi2jkci.us-east-1.es.amazonaws.com'

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

# Helper Function: Get random restaurant recommendations from OpenSearch (Elasticsearch)
def search_restaurants(cuisine):
    query = {
        "size": 5,  # Request 5 restaurants from OpenSearch
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        }
    }
    response = requests.post(opensearch_url, auth=HTTPBasicAuth(master_user, master_password), headers=headers, data=json.dumps(query))
    
    result = response.json()
    if 'hits' in result and result['hits']['hits']:
        # Return up to 5 restaurant IDs
        return [hit['_source']['RestaurantID'] for hit in result['hits']['hits']]
    
    return []

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

def format_recommendation_message(restaurants, cuisine, number_of_people, time):
    # Build a formatted string with restaurant recommendations
    email_content = f"Hello! Here are my {cuisine} restaurant suggestions for {number_of_people} people, for today at {time}:\n"
    
    for restaurant in restaurants:
        email_content += f"Restaurant Name: {restaurant['Name']}\n"
        email_content += f"Address: {restaurant['Address']}\n"
        email_content += f"Rating: {restaurant['Rating']}\n\n"
    
    return email_content

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

    # Step 2: Get random restaurants from OpenSearch (5 restaurants)
    restaurant_ids = search_restaurants(cuisine)

    if not restaurant_ids:
        try:
            send_email(email, "No Recommendation", f"Sorry, we couldn't find any recommendations for {cuisine}.")
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except botocore.exceptions.ClientError as e:
            # Handle specific email sending errors (SES related)
            print(f"Failed to send email to {email}. Error: {str(e)}")
            return {"statusCode": 400, "body": "No Email found."}
        except Exception as e:
            # Handle any other general errors
            print(f"An unexpected error occurred while sending email: {str(e)}")
            return {"statusCode": 400, "body": "No Email found."}
        return {"statusCode": 200, "body": "No restaurants found."}

    # Step 3: Get restaurant details for each restaurant from DynamoDB
    restaurants = [get_restaurant_details(restaurant_id) for restaurant_id in restaurant_ids]

    # Step 4: Format the recommendations

    try:
        # Step 5: Send email with the recommendations
        send_email(email, f"Recommendations for {cuisine}", format_recommendation_message(restaurants,cuisine,message['Number_of_People'],message['Dining_Time']))
    
    except botocore.exceptions.ClientError as e:
        # Handle specific email sending errors (SES related)
        print(f"Failed to send email to {email}. Error: {str(e)}")
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        return {"statusCode": 400, "body": "No Email found."}
    except Exception as e:
        # Handle any other general errors
        print(f"An unexpected error occurred while sending email: {str(e)}")
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        return {"statusCode": 400, "body": "No Email found."}


    # Step 6: Delete the message from the SQS queue after processing
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    return {"statusCode": 200, "body": "Recommendations sent successfully."}
