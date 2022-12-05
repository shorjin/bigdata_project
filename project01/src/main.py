# Allows us to connect to the data source and pulls the information
from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import json
import argparse
import sys
import os
import time
#This comes from the documentation:
#https://dev.socrata.com/foundry/data.cityofnewyork.us/8m42-w767


parser = argparse.ArgumentParser(description='Fire Incident Dispatch Data')
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
parser.add_argument('--num_pages', type=int,required=False, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])


DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]


if __name__ == '__main__': 
    try:
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                json={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    },
                    "mappings": {
                        
                        "properties": {
                            "starfire_incident_id": {"type": "keyword"},
                            "incident_datetime": {"type": "date"},
                            "incident_borough": {"type": "keyword"},
                            "highest_alarm_level": {"type": "keyword"},
                            "incident_classification": {"type": "keyword"},
                            "incident_classification_group": {"type": "keyword"},
                            "dispatch_response_seconds_qy": {"type": "float"}, 
                            "incident_travel_tm_seconds_qy": {"type": "float"},
                            "engines_assigned_quantity": {"type": "float"},
                        }
                    },
                }
            )
        resp.raise_for_status()
        print(resp.json())
    except Exception as e:
        print("Index already exists! Skipping")
        
    start_processing_time = time.time()
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
    total_record_count = client.get(DATASET_ID, select = "COUNT(*)")
    total=int(total_record_count[0]["COUNT"])


    if args.num_pages is None:
        #if user don't input num-pages, the script will automatically calculate the max of num-pages(only round up) and extract all records from the dataset 
        #default=-(-total//args.page_size)
        args.num_pages=-(-total//args.page_size)
    else:
        args.num_pages

    for i in range(0, args.num_pages): 
        start = time.time()
        rows=client.get(DATASET_ID,limit=args.page_size,offset=i*(args.page_size),where="starfire_incident_id" !=None and "incident_datetime"!=None,order="incident_datetime DESC")
        es_rows=[]

        
        for row in rows:
            try:
                # Convert
                es_row = {}
                es_row["starfire_incident_id"] = row["starfire_incident_id"]
                es_row["incident_datetime"] = row["incident_datetime"]
                es_row["incident_borough"] = row["incident_borough"]
                es_row["highest_alarm_level"] = row["highest_alarm_level"]
                es_row["incident_classification"] = row["incident_classification"]
                es_row["incident_classification_group"] = row["incident_classification_group"]
                es_row["dispatch_response_seconds_qy"] = float(row["dispatch_response_seconds_qy"]) 
                es_row["incident_travel_tm_seconds_qy"] = float(row["incident_travel_tm_seconds_qy"])
                es_row["engines_assigned_quantity"] = float(row["engines_assigned_quantity"])
               
            except Exception as e:
                print (f"Error!: {e}, skipping row: {row}")
                continue
            es_rows.append(es_row)
            
        
        bulk_upload_data = ""
        for line in es_rows:
            
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
        
        
        try:
            # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/_bulk",
                # We upload es_row to Elasticsearch
                        data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            
                
            # If it fails, skip that row and move on.
        except Exception as e:
            print(f"Failed to insert in ES: {e}")
        end = time.time()
        print(f"*****Page {i+1} finished in {round(end - start, 2)} seconds*****\n")  
    end_processing_time = time.time()
    print("All data are uploaded to Elasticsearch")
    print(f"Total Records in {DATASET_ID} dataset: {total}")
    print(f"Total processing Time:{round(end_processing_time - start_processing_time, 2)} seconds")

