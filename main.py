import functions_framework
import requests
import json
import os
import logging
from google.cloud import storage
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO)

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
    
@functions_framework.http
def insert_datasets_to_postgres(request):
    """Cloud Function to read JSON from GCS and insert into PostgreSQL."""

    bucket_name = "data-list-111"
    file_name = "gfw_datasets_list.json"
    conn = None
    cur = None

    try:
        # Read JSON from GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        json_string = blob.download_as_string().decode("utf-8")
        data = json.loads(json_string)

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            database=os.environ.get("DB_NAME")
        )
        cur = conn.cursor()

        # Insert data into the "datasets" table
        for item in data:
            dataset_name = item.get("dataset_name")
            geographic_coverage = item.get("geographic_coverage")

            if dataset_name and geographic_coverage:
                cur.execute(
                    """
                    INSERT INTO datasets (dataset_name, geographic_coverage)
                    VALUES (%s, %s)
                    ON CONFLICT (dataset_name) DO UPDATE
                    SET geographic_coverage = EXCLUDED.geographic_coverage;
                    """,
                    (dataset_name, geographic_coverage)
                )
            else:
                logging.warning(f"Skipping item with missing dataset_name or geographic_coverage: {item}")

        conn.commit()
        return "Datasets inserted into PostgreSQL successfully.", 200

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error: {e}")
        return f"Error: {e}", 500

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()