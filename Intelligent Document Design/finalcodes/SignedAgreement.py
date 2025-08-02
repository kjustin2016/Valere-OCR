import os
import boto3
import botocore
import json
from dotenv import load_dotenv
from textractcaller.t_call import call_textract
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print, get_string)
import trp.trp2 as t2
from tabulate import tabulate
import psycopg2
from collections import Counter
import time
import re

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
bucket_name = 'capstone-intelligent-document-processing'
db_endpoint = os.getenv("DB_ENDPOINT")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
maxSize = 10*1024*1024


s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

textract = boto3.client('textract',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=aws_region)

def getObjectNames(bucket_name):
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name}
    page_iterator = paginator.paginate(**operation_parameters)

    object_keys = []
    page_iterator = iter(page_iterator)

    #if want to run this on a specific range of documents, use the code below and adjust page to
    # which of the three pages (page 1: 0-999, page 2: 1000-1999, etc.) and adjust >= 7 to the
    #number of documents you want it to grab

    # page = next(page_iterator, None)
    # if page and 'Contents' in page:
    #     for item in page['Contents']:
    #         object_keys.append(item['Key'])
    #         if len(object_keys) >= 7:
    #             break
    # return object_keys

    #code below runs it on the entire database

    for page in page_iterator:
        if 'Contents' in page:
            for item in page['Contents']:
                object_keys.append(item['Key'])
    return object_keys

def get_db_connection():
    return psycopg2.connect(host=db_endpoint,
                            port=db_port,
                            database=db_name,
                            user=db_user,
                            password=db_pass,
                            sslrootcert="SSLCERTIFICATE")

def detect_data(blocks):
    dataResults = {}
    dataResults["confidence"] = {}
    dataResults["document_data"] = {
        "name": "",
        "date": "",
    }

    for block in blocks:
        if block.get("BlockType") == "SIGNATURE":
            dataResults["document_data"]["signature"] = ("Present")

        if block.get("BlockType") == "LINE":
            text = block.get("Text", "")
            if "Signed by customer:" in text or "Firmado por el cliente:" in text:
                parts = text.split(":", 1)
                if len(parts) > 1:
                    name = parts[1].strip()
                    dataResults["document_data"]["name"] = name
                else:
                    dataResults["document_data"]["name"] = ""

            if "Date" in text or "Fecha" in text:
                date_match = re.search(r"(Date|Fecha)\s*[:]?\s*(\d{2}/\d{2}/\d{4} \d{2}:\d{2} CMT)", text)
                if date_match:
                    date = date_match.group(2).strip()
                    dataResults["document_data"]["date"] = date
                else:
                    dataResults["document_data"]["date"] = ""
    return dataResults

testing = getObjectNames(bucket_name)
connection = get_db_connection()
cursor = connection.cursor()

for index, docNames in enumerate(testing):
    print(index)
    sizeCheck = s3.head_object(Bucket=bucket_name, Key=docNames)
    size = sizeCheck['ContentLength']

    if not docNames.endswith(".pdf"):
        continue

    if size > maxSize:
        continue

    if "026fc6e3b3eb47b9894ccb490be6885c_OUT_PATIENT_2024_07_13_07_04_56_0e6c137980dd4f789d6793b35f8172eb_INSURANCECARD_capturepng.null.png" in docNames:
        continue

    try:
        response = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': (docNames)}},
        FeatureTypes=["FORMS", "SIGNATURES"]
        )

        job_id = response['JobId']
        while True:
            result = textract.get_document_analysis(JobId=job_id)
            if result["JobStatus"] in ["SUCCEEDED", "FAILED"]:
                break
            time.sleep(2)
        
        all_blocks = []
        next_token = None
        first_page_metadata = None

        while True:
            if next_token:
                result = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
            else:
                result = textract.get_document_analysis(JobId=job_id)
                first_page_metadata = result.get("DocumentMetadata")
        
            all_blocks.extend(result['Blocks'])
            next_token = result.get('NextToken')
            if not next_token:
                break

        full_response = {
            "Blocks": all_blocks,
            "DocumentMetadata": first_page_metadata if first_page_metadata else {},
            "JobStatus": response.get("JobStatus", "UNKNOWN")
        }

        queryData={}
        queryData["confidence"]={}
        queryData["document_data"]={}
        emptyCount = 0
        keyCount = 0
        
        queryData = detect_data(all_blocks)
        for key, value in queryData["document_data"].items():
            if key:
                keyCount += 1
            if value == "":
                emptyCount += 1
        
        confidence_score = (keyCount-emptyCount)/keyCount
        queryData["confidence"] = {"confidence_score": confidence_score}
        # print(json.dumps(queryData, indent=4))

        # print(emptyCount)# manually checking how many values are empty
        # print(keyCount-emptyCount)
        # print(keyCount)
        # print(confidence_score)

        # print(json.dumps(queryData, indent=4))# checking the output

        cursor.execute("INSERT INTO signedagreement (document_key, json, confidence_score) VALUES (%s, %s, %s)",(docNames, json.dumps(queryData, indent=4), confidence_score))
        connection.commit()
    except textract.exceptions.UnsupportedDocumentException as e:
        print(f"Unsupported document format for file: {docNames}, skipping.")
        continue

    except botocore.exceptions.ClientError as e:
        print(f"A client error occurred for {docNames}: {e}")
        continue

    except Exception as e:
        print(f"An unexpected error occurred for {docNames}: {e}")
        continue
