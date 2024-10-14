import boto3
import requests
import json
import os
import time
from urllib.parse import urlencode
from decimal import Decimal
import datetime

# Yelp API endpoint and headers
API_KEY = 'MXUHTO0km8vyS-yuZe-Y1KJiIE1v_eeDWEY3PpkFqJvDjMe870cAVFxbV4PTb1AeZLKM4LL7AtwLLT-Pn6uyKSUYobxWcOLB-it1dmG9jHWAeiEjyWFTp2GP744NZ3Yx'  # Set the Yelp API key in Lambda's environment variables

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
YELP_URL = f'{API_HOST}{SEARCH_PATH}'
HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
}

# AWS DynamoDB setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')  # Ensure the table exists in DynamoDB

# Function to call Yelp API
def get_yelp_data(term, location, offset=0, limit=50):
    """Fetches restaurant data from Yelp API."""
    url_params = {
        'term': term,
        'location': location,
        'limit': limit,
        'offset': offset
    }
    encoded_url_params = urlencode(url_params)
    yelp_request_url = f"{YELP_URL}?{encoded_url_params}"
    
    try:
        response = requests.get(yelp_request_url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Yelp API request failed: {e}")
        return None

# Function to save data to DynamoDB
def save_to_dynamodb(business, cuisine):
    """Saves a single restaurant record to DynamoDB."""
    try:
        table.put_item(
            Item={
                'BusinessID': business['id'],
                'Name': business['name'],
                'Address': ', '.join(business['location']['display_address']),
                'Cuisine': cuisine,  # Add the cuisine type here
                'Coordinates': json.dumps(business['coordinates']),  # Store as JSON string
                'NumberOfReviews': Decimal(business['review_count']),
                'Rating': Decimal(round(business['rating'] * 2) / 2),  # Round to nearest .0 or .5
                'ZipCode': business['location']['zip_code'],
                'InsertedAtTimestamp': str(datetime.datetime.now())
            }
        )
        print(f"Saved {business['name']} ({cuisine}) to DynamoDB.")
    except Exception as e:
        print(f"Error saving {business['name']} to DynamoDB: {e}")

# Function to scrape Yelp for multiple cuisines
def scrape_cuisines():
    """Fetches data for multiple cuisines and saves to DynamoDB."""
    cuisines = ['Italian', 'Mexican', 'Chinese', 'Japanese', 'Indian']
    location = 'Manhattan, NY'
    
    for cuisine in cuisines:
        print(f"Scraping for {cuisine} restaurants in {location}")
        offset = 0
        total_scraped = 0
        
        while total_scraped < 1000:  # Scrape up to 1000 restaurants per cuisine
            data = get_yelp_data(f"{cuisine} restaurant", location, offset)
            
            if not data or 'businesses' not in data:
                break  # Stop if we hit an error or no more data
            
            businesses = data['businesses']
            for business in businesses:
                save_to_dynamodb(business, cuisine)  # Pass cuisine as an argument
            
            total_scraped += len(businesses)
            offset += 50  # Move to the next set of businesses
            
            time.sleep(1)  # Add a delay to avoid hitting Yelp's rate limits
        
        print(f"Completed scraping for {cuisine}")

def lambda_handler(event, context):
    """Lambda handler to trigger Yelp scraping."""
    scrape_cuisines()
    return {
        'statusCode': 200,
        'body': json.dumps('Yelp data scraping completed.')
    }

