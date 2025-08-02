import os
import boto3
import json
from dotenv import load_dotenv
from textractcaller.t_call import call_textract
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print, get_string)
import trp.trp2 as t2
from tabulate import tabulate
import psycopg2

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
db_endpoint = os.getenv("DB_ENDPOINT")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
bucket_name = 'capstone-intelligent-document-processing'

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
    
    page = next(page_iterator, None)
    if page and 'Contents' in page:
        for item in page['Contents']:
            object_keys.append(item['Key'])
            if len(object_keys) >= 7:
                break
    return object_keys

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
connection = get_db_connection()
cursor = connection.cursor()

for docNames in testing:
    if not docNames.endswith(".pdf"):
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
      
      d2 = t2.TDocumentSchema().load(response2)
      page = d2.pages[0]
      query_answers2 = d2.get_query_answers(page=page)

      queryData={}

      count=len(query_answers)
      for i in range(count):
          (a,b,c) = query_answers[i]
          a = a.split("the ",1)[1]
          a = a.split("?",1)[0]
          queryData[a] = c
      
      count2=len(query_answers2)
      print(count2)
      for i in range(count2):
          (a,b,c) = query_answers2[i]
          a = a.split("the ",1)[1]
          a = a.split("?",1)[0]
          queryData[a] = c
      
      cursor.execute("INSERT INTO documents (document_key, json) VALUES (%s, %s)",(docNames, json.dumps(queryData, indent=4)))
      connection.commit()
