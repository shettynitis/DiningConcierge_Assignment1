import boto3
import requests
from requests.auth import HTTPBasicAuth
import json

# AWS Credentials and OpenSearch details
region = 'us-east-1'
service = 'es'

# OpenSearch domain endpoint (replace with your actual domain endpoint)
opensearch_url = 'https://search-dining-domain-xumyoad3xjnwxcyh33k2aveezm.us-east-1.es.amazonaws.com'

# Master user credentials (username and password)
master_user = 'NitishaShetty'
master_password = 'Password@98'

# Updated OpenSearch URL to include the index name and type
index_name = "restaurants"  # Name of the OpenSearch index
document_type = "_doc"  # OpenSearch 7.x and later uses '_doc' as the document type
opensearch_url = f'{opensearch_url}/{index_name}/{document_type}'

headers = { "Content-Type": "application/json" }

# Function to store restaurant data in OpenSearch
def store_in_opensearch(restaurant_id, cuisine):
    document = {
        "RestaurantID": restaurant_id,
        "Cuisine": cuisine
    }
    
    # Send request with Basic Authentication (using master user credentials)
    response = requests.post(
        opensearch_url, 
        auth=HTTPBasicAuth(master_user, master_password), 
        headers=headers, 
        data=json.dumps(document)
    )
    
    if response.status_code == 201:
        print(f"Stored {restaurant_id} in OpenSearch successfully.")
    else:
        print(f"Failed to store {restaurant_id} in OpenSearch. Error: {response.text}")
    
    return response

def lambda_handler(event, context):
    # Example data (replace with actual scraped data)
    restaurant_id = "1235"
    cuisine = "Italian"
    
    # Store data in OpenSearch
    response = store_in_opensearch(restaurant_id, cuisine)
    
    # Check if the request was successful
    if response.status_code == 201:
        return {
            'statusCode': 200,
            'body': json.dumps('Data stored in OpenSearch successfully!')
        }
    else:
        return {
            'statusCode': response.status_code,
            'body': json.dumps(f"Failed to store data in OpenSearch. Response: {response.text}")
        }
