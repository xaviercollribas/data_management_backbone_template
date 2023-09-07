import os
from google.cloud import storage
import pandas as pd
import argparse
from sqlalchemy import create_engine

execution_path = os.environ['DMB_EXECUTION_PATH']

engine = create_engine('postgresql://xavier:pwd@localhost:5432/formatted_zone')

def load_csv_files_to_postgres(storage_client, bucket_name):
   # Initialize GCS client
   bucket = storage_client.get_bucket(bucket_name)

   # List objects in the GCS bucket
   blobs = list(bucket.list_blobs())

   for blob in blobs:
       # Extract the file name from the GCS blob
       file_name = os.path.basename(blob.name)
       print(file_name)
       # Define the PostgreSQL table name based on the file name
       table_name = os.path.splitext(file_name)[0]

       # Download the CSV file from GCS to a temporary location
       tmp_csv_path = f"/tmp/{file_name}"
       blob.download_to_filename(tmp_csv_path)

       df = pd.read_csv(tmp_csv_path)
       df.to_sql(table_name, engine, if_exists='replace', index=False) 

       print(f"{file_name} succesfully loaded to Formatted Zone")
       # Commit and close the database connection for each file


def main():
    parser = argparse.ArgumentParser(description="Load CSV files from Google Cloud Storage to PostgreSQL.")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    args = parser.parse_args()

    storage_client = storage.Client.from_service_account_json(f'{execution_path}/creds.json')
    load_csv_files_to_postgres(storage_client, args.bucket) 

if __name__ == "__main__":
    main()
