import os
import boto3
import json
from dotenv import load_dotenv
from textractcaller.t_call import call_textract
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print, get_string)
import trp.trp2 as t2
from tabulate import tabulate
import psycopg2
from collections import Counter

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
maxSize = 10 * 1024 * 1024


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

    page = next(page_iterator, None)
    if page and 'Contents' in page:
        for item in page['Contents']:
            object_keys.append(item['Key'])
            if len(object_keys) >= 36:
                break
    return object_keys

    #code below runs it on the entire database

    # for page in page_iterator:
    #     if 'Contents' in page:
    #         for item in page['Contents']:
    #             object_keys.append(item['Key'])
    # return object_keys

def get_db_connection():
    return psycopg2.connect(host=db_endpoint,
                            port=db_port,
                            database=db_name,
                            user=db_user,
                            password=db_pass,
                            sslrootcert="SSLCERTIFICATE")


testing = getObjectNames(bucket_name)

# print(len(testing))# testing how many objects were retreieved

connection = get_db_connection()
cursor = connection.cursor()

for docNames in testing:
    sizeCheck = s3.head_object(Bucket=bucket_name, Key=docNames)
    size = sizeCheck['ContentLength']

    if docNames.endswith(".pdf"):
        continue

    if size > maxSize:
        continue

    response = textract.analyze_document(
    Document={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': (docNames)}},
    FeatureTypes=["QUERIES"],
    QueriesConfig={"Queries": [
    {"Text": "What is the Member Name", "Alias": "MEMBER_NAME"},
    {"Text": "What is the Member ID?", "Alias": "MEMBER_ID"},
    {"Text": "Who is the PCP?", "Alias": "PCP"},
    {"Text": "What is the phone number of the PCP?", "Alias": "PCP_PHONE"},
    {"Text": "What is the medical insurance provider?", "Alias": "MEDICAL_PROVIDER"},
    {"Text": "What is the effective date?", "Alias": "EFFECTIVE_DATE"},
    {"Text": "What is the Group No.?", "Alias": "GROUP_NUMBER"},
    {"Text": "What is the plan type?", "Alias": "PLAN_TYPE"},
    {"Text": "What is the BIN?", "Alias": "BIN"},
    {"Text": "What is the Rx PCN?", "Alias": "RX-PCN"},
    {"Text": "What is the Generic Copay?", "Alias": "GENERIC_COPAY"},
    {"Text": "What is the Brand Copay?", "Alias": "BRAND_COPAY"},
    {"Text": "What is the Specialty Copay?", "Alias": "SPECIALTY_COPAY"},
    {"Text": "What is the Emergency Room Percentage?", "Alias": "EMERGENCY_ROOM_PERCENTAGE"},
    {"Text": "What is the PCP Copay?", "Alias": "PCP_COPAY"}
    ]}
    )

    d = t2.TDocumentSchema().load(response)
    page = d.pages[0]
    query_answers = d.get_query_answers(page=page)
    count=len(query_answers)

    queryData={}
    queryData["confidence"]={}
    queryData["document_data"]={}
    emptyCount = 0
    keyCount = 0
    
    for i in range(count):
        (a,b,c) = query_answers[i]
        a = a.split("the ",1)[1]
        a = a.split("?",1)[0]
        queryData["document_data"][a] = c

    for key, value in queryData["document_data"].items():
        if key:
            keyCount += 1
        if value == "":
            emptyCount += 1
    
    confidence_score = (keyCount-emptyCount)/keyCount
    queryData["confidence"] = {"confidence_score": confidence_score}
    # countTesting = Counter(queryData.values())# using counter to get number of empty values
    # countTestingTest = countTesting[""]

    # print(emptyCount)# manually checking how many values are empty
    # print(keyCount-emptyCount)
    # print(keyCount)
    # print(confidence_score)

    print(json.dumps(queryData, indent=4))# checking the output

    # cursor.execute("INSERT INTO insurance1 (document_key, json, confidence_score) VALUES (%s, %s, %s)",(docNames, json.dumps(queryData, indent=4), confidence_score))
    # connection.commit()
