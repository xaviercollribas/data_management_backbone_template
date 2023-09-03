import requests
import os
import json
import argparse
import sys

# ENVIRONMENT VARIABLES

execution_path = os.environ['DMB_EXECUTION_PATH']

def retrieve_data(endpoint):
    response = requests.get(endpoint)
    correct_request = False
    if response.status_code == 200:
        print("API 1: CORRECT RESPONSE")
        correct_request = True
    else:
        print(f"API 1: ERROR. ERROR CODE: {response.status_code}")

    if correct_request:
        return response
    else: 
        return None


def generate_file(FILENAME, TEMPORAL_LANDING_ZONE_PATH):
    file_name = f"{FILENAME}.json"
    file_path = os.path.join(TEMPORAL_LANDING_ZONE_PATH, file_name)
    return file_path

def store_file(path, content):
    json_format = json.dumps(content, ensure_ascii=True)
    with open(path, 'w') as file:
        s = file.write(json_format)    
    return s


def main():
    parser = argparse.ArgumentParser(description="Process command line arguments.")
    
    parser.add_argument("--api_endpoint", required=True, help="API endpoint URL")
    parser.add_argument("--temporal_landing_zone_path", required=True, help="Temporal landing zone path")
    parser.add_argument("--filename", required=True, help="Filename")

    args = parser.parse_args()

    API_ENDPOINT = args.api_endpoint
    TEMPORAL_LANDING_ZONE_PATH = args.temporal_landing_zone_path
    FILENAME = args.filename


    print(f"Fetching data from {API_ENDPOINT}")
    response = retrieve_data(API_ENDPOINT)
    if response != None:
        print("Generating file")
        file_path = generate_file(FILENAME, TEMPORAL_LANDING_ZONE_PATH)
        print(f"Storing file in {file_path}")
        s = store_file(file_path, response.json())
        if s > 0:
            print("Data stored in temporal landing zone correctly")
        else:
            print("There was an error storing the data in the temporal landing zone.")
    else:
        sys.exit("There was an error while retrieving data from {API_ENDPOINT}")
                
if __name__ == '__main__':
    main()