import base64
import csv
import io
import json
import logging
import time
from collections import OrderedDict
from google.cloud import storage

def fun(event, context):
    message_dict = OrderedDict(event['data'])
    resource = json.load(open("resources.json"))
    bucket_name = resource['bucket_name']
    file_name = resource['file_name']
    log_file_name = resource['log_file_name']

    # Create storage client and get bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Retry mechanism
    max_retries = 3
    retry_count = 0
    while True:
        try:
            # Check if CSV file exists and read existing data
            csv_blob = bucket.blob(file_name)
            if csv_blob.exists():
                existing_data = csv_blob.download_as_string().decode('utf-8')
                csv_data = io.StringIO(existing_data)
                reader = csv.DictReader(csv_data)
                rows = list(reader)
            else:
                csv_data = io.StringIO()
                rows = []

            # Append new row to the existing rows
            rows.append(message_dict)

            # Write all rows to the CSV file
            csv_data = io.StringIO()
            writer = csv.DictWriter(csv_data, fieldnames=message_dict.keys())
            writer.writeheader()
            writer.writerows(rows)
            csv_blob.upload_from_string(csv_data.getvalue(), content_type='text/csv')

            # Save logs to file
            log_blob = bucket.blob(log_file_name)
            if log_blob.exists():
                existing_log_data = log_blob.download_as_string().decode('utf-8')
                log_data = existing_log_data + f"\n{time.asctime() } : Order ID {message_dict['Order ID']} processed successfully"
            else:
                log_data = f"{time.asctime() } : Order ID {message_dict['Order ID']} processed successfully"
            log_blob.upload_from_string(log_data)

            logging.info(log_data)

            break  # exit the loop if everything is successful

        except Exception as e:
            retry_count += 1
            if retry_count <= max_retries:
                logging.warning(f"Error uploading files: {e}. Retrying ({retry_count}/{max_retries})...")
                time.sleep(5)  # wait for 5 seconds before retrying
            else:
                logging.error(f"Max retry count exceeded: {e}")
                break  # exit the loop if max retry count is exceeded

    # Error handling for CSV parsing
    try:
        csv_data.seek(0)  # move the cursor to the beginning of the StringIO object
        parsed_csv_data = csv.DictReader(csv_data)
    except Exception as e:
        logging.error(f"Error parsing CSV file: {e}")
