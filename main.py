import functions_framework
import requests
import json
import logging
from google.cloud import storage

@functions_framework.http
def fetch_and_save_gfw_datasets(request):

    try:
        api_url = "https://data-api.globalforestwatch.org/datasets"
        response = requests.get(api_url)
        response.raise_for_status()
        datasets = response.json()['data'] 

        extracted_data = []
        for dataset in datasets:
            dataset_name = dataset['dataset']
            geographic_coverage = dataset['metadata'].get('geographic_coverage', 'N/A')
            extracted_data.append({'dataset_name': dataset_name, 'geographic_coverage': geographic_coverage})

        bucket_name = "data-list-111"  
        file_name = "gfw_datasets_list.json"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(extracted_data), content_type="application/json")

        return f"Dataset data saved to gs://{bucket_name}/{file_name}", 200

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from GFW API: {e}")
        return f"Error fetching data: {e}", 500
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON response: {e}")
        return f"Error decoding JSON: {e}", 500
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return f"An unexpected error occurred: {e}", 500
    