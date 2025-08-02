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
    #         if len(object_keys) >= 61:
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


testing = getObjectNames(bucket_name)

# print(len(testing))# testing how many objects were retreieved

connection = get_db_connection()
cursor = connection.cursor()

# 01424ca6ba424ce4940ad69af1a779b8_292acc2c14224f0180b7ef8991772cb3_FACESHEET_image_picker_03352D681851447DA6DBB8B3BE746E43354000000137CCC1D5Cjpg_FACESHEET_image_picker_03352D681851447DA6DBB8B3BE746E43354000000137CCC1D5Cjpg.null.jpg
for index, docNames in enumerate(testing):
    print(index)
    sizeCheck = s3.head_object(Bucket=bucket_name, Key=docNames)
    size = sizeCheck['ContentLength']

# Check the file extension
    if not docNames.endswith(('.jpg', '.jpeg', '.png', '.tiff')):
        continue  # Skip if it's not an image file

# Check the file size
    response = s3.head_object(Bucket=bucket_name, Key=docNames)
    size = response['ContentLength']

    if size > maxSize:
        continue  # Skip if the file is too large

    if "026fc6e3b3eb47b9894ccb490be6885c_OUT_PATIENT_2024_07_13_07_04_56_0e6c137980dd4f789d6793b35f8172eb_INSURANCECARD_capturepng.null.png" in docNames:
        continue

    try:
        response = textract.analyze_document(
        Document={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': (docNames)}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={"Queries": [
        {"Text": "What is the Patient Name?", "Alias": "patientname"},
        {"Text": "What is the Patient Date of Birth?", "Alias": "patientdob"},
        {"Text": "What is the Patient Address?", "Alias": "patientaddress"},
        {"Text": "What is the Patient sex?", "Alias": "patientsex"},
        {"Text": "What is the Patient Ethnicity?", "Alias": "patientethnicity"},
        {"Text": "What is the Patient citizenship?", "Alias": "patientcitizenship"},
        {"Text": "What is the Patient Race?", "Alias": "patientrace"},
        {"Text": "What is the Patient Phone Number?", "Alias": "patientphone"},
        {"Text": "What is the Admitting Provider Name?", "Alias": "admittingname"},
        {"Text": "What is the Attending Provider Telephone Number?", "Alias": "attendphone"},
        {"Text": "What is the Attending Provider Name?", "Alias": "attendname"},
        {"Text": "What is the Refering physician?", "Alias": "refphysician"},
        {"Text": "What is the admitting diagnosis?", "Alias": "admittingdiagnosis"},
        {"Text": "What is the Encounter Date?", "Alias": "encounterdate"},
        {"Text": "What is the MRN?", "Alias": "mrn"}
        ]}
        )

        response2 = textract.analyze_document(
        Document={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': (docNames)}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={"Queries": [
        {"Text": "What is the Hospital Account number?", "Alias": "hospitalaccountnumber"},
        {"Text": "What is the Contact Serial number?", "Alias": "contactserialnumber"},
        {"Text": "What is the Patient insurance provider?", "Alias": "patientinsuranceprovider"},
        {"Text": "What is the insurance Subscriber name?", "Alias": "insurancesubscribername"},
        {"Text": "What is the Patient insurance group number?", "Alias": "patientinsurancegroupnumber"},
        {"Text": "What is the Patient insurance Subscriber Id?", "Alias": "patientinsurancesubscriberid"},
        {"Text": "What is the Patient insurance type?", "Alias": "patientinsurancetype"},
        {"Text": "What is the Patient insurance plan?", "Alias": "patientinsuranceplan"},
        {"Text": "What is the Patient relationship to insurance Subscriber?", "Alias": "patientrelationshiptoinsurancesubscriber"},
        {"Text": "What is the insurance verifiaction status?", "Alias": "insuranceverificationstatus"},
        {"Text": "What is the Garuntor Name?", "Alias": "garuntorname"},
        {"Text": "What is the Garuntor relation to patient?", "Alias": "garuntorrelationtopatient"},
        {"Text": "What is the Garuntor Id?", "Alias": "garuntorid"},
        {"Text": "What is the Garuntor Address?", "Alias": "garuntoraddress"},
        {"Text": "What is the Garuntor Phone number?", "Alias": "garuntorphone"}
        ]}
        )


        d = t2.TDocumentSchema().load(response)
        page = d.pages[0]
        query_answers = d.get_query_answers(page=page)
        count=len(query_answers)

        d2 = t2.TDocumentSchema().load(response2)
        page = d2.pages[0]
        query_answers2 = d2.get_query_answers(page=page)
        count2=len(query_answers2)


        queryData={}
        queryData["confidence"]={}
        queryData["document_data"]={}
        emptyCount = 0
        keyCount = 0

        # asdf
        
        for i in range(count):
            (a,b,c) = query_answers[i]
            a = a.split("the ",1)[1]
            a = a.split("?",1)[0]
            queryData["document_data"][a] = c
        
        for i in range(count2):
            (a,b,c) = query_answers2[i]
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

        # print(json.dumps(queryData, indent=4))# checking the output

        cursor.execute("INSERT INTO facesheet (document_key, json, confidence_score) VALUES (%s, %s, %s)",(docNames, json.dumps(queryData, indent=4), confidence_score))
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
