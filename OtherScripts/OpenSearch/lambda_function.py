import boto3
import requests
from requests.auth import HTTPBasicAuth
import json

# AWS OpenSearch details
region = 'us-east-1'
service = 'es'

# OpenSearch domain URL
opensearch_url = 'https://search-diningdomain-meft6dgd4aul4ho2pyjhi2jkci.us-east-1.es.amazonaws.com'

# Master user credentials (username and password)
master_user = 'NitishaShetty'
master_password = 'Password@98'

# OpenSearch index
index_name = "restaurants"
document_type = "_doc"
opensearch_url = f'{opensearch_url}/{index_name}/{document_type}'

# Headers
headers = { "Content-Type": "application/json" }

# DynamoDB table name
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')  # Ensure you are using the correct table name

# Function to store restaurant data in OpenSearch
def store_in_opensearch(restaurant_id, cuisine):
    document = {
        "RestaurantID": restaurant_id,
        "Cuisine": cuisine
    }
    response = requests.post(opensearch_url, auth=HTTPBasicAuth(master_user, master_password), headers=headers, data=json.dumps(document))
    
    if response.status_code == 201:
        print(f"Stored {restaurant_id} in OpenSearch successfully.")
    else:
        print(f"Failed to store {restaurant_id} in OpenSearch. Error: {response.text}")
    
    return response

def lambda_handler(event, context):
    # Fetch data from DynamoDB
    response = table.scan()  # Fetch all data from DynamoDB table
    items = response.get('Items', [])
    
    # Iterate through each restaurant from DynamoDB
    for restaurant in items:
        restaurant_id = restaurant['BusinessID']
        cuisine = restaurant.get('Cuisine', 'Unknown')  # Assuming you have the 'Cuisine' field
    
        # Store partial information in OpenSearch
        store_in_opensearch(restaurant_id, cuisine)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data stored in OpenSearch successfully!')
    }
