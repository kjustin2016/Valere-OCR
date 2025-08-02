import os
import boto3
import json
import time
import re
import PyPDF2
from io import BytesIO
from dotenv import load_dotenv

# =============== CONFIGURATION ===============
# Set entity tag here for easy changing
ENTITY_TAG = "c8f7ae014002c6feefa5686713770592"  # Replace with the desired entity tag
# ============================================

# Load environment variables
load_dotenv()

# Initialize AWS clients
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
index_1 = None

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
textract = boto3.client('textract', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

def get_s3_bucket_object_by_index(bucket, index=None):
    if index is None:
        return None

    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    all_object_keys = []
    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            object_keys = [item['Key'] for item in page['Contents']]
            all_object_keys.extend(object_keys)

    if len(all_object_keys) > index:
        return all_object_keys[index]
    else:
        print(f"Index {index} is out of range. Total objects available: {len(all_object_keys)}")
    return None

def get_s3_bucket_object_by_tag(bucket, entity_tag):
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            for item in page['Contents']:
                object_key = item['Key']
                try:
                    metadata = s3.head_object(Bucket=bucket, Key=object_key)
                    if 'ETag' in metadata and metadata['ETag'].strip('"') == entity_tag:
                        return object_key
                except Exception as e:
                    print(f"Error fetching metadata for {object_key}: {e}")
    return None

def extract_text_from_pdf(bucket, object_key):
    """Extract text directly from PDF using PyPDF2"""
    try:
        # Download the PDF file from S3
        response = s3.get_object(Bucket=bucket, Key=object_key)
        pdf_content = response['Body'].read()
        
        # Process the PDF
        pdf_file = BytesIO(pdf_content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        # Extract text from all pages
        all_text = ""
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            all_text += page.extract_text() + "\n\n"
        
        return all_text
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return None

def textract_extract_text(bucket, object_key):
    try:
        # Check file extension
        file_extension = object_key.split('.')[-1].lower()

        if file_extension in ['png', 'jpg', 'jpeg']:  # Handle image files
            response = textract.analyze_document(
                Document={'S3Object': {'Bucket': bucket, 'Name': object_key}},
                FeatureTypes=["FORMS", "TABLES", "SIGNATURES"]
            )
        elif file_extension == 'pdf':  # Handle PDF files
            response = textract.start_document_analysis(
                DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': object_key}},
                FeatureTypes=["FORMS", "TABLES", "SIGNATURES"]
            )
            job_id = response['JobId']
            print(f"Started asynchronous job for PDF with JobId: {job_id}")

            while True:
                result = textract.get_document_analysis(JobId=job_id)
                status = result['JobStatus']
                if status in ['SUCCEEDED', 'FAILED']:
                    break
                print("Waiting for Textract to complete the analysis...")
                time.sleep(5)

            if status == 'SUCCEEDED':
                response = result
            else:
                raise ValueError("Textract document analysis failed.")
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

        return response

    except Exception as e:
        print(f"Error in textract_extract_text: {e}")
        return None

def structure_text(response):
    # Extract the text from the Textract response dictionary
    text = ""
    blocks = response.get('Blocks', [])
    
    # Iterate over the blocks to extract text
    for block in blocks:
        if block['BlockType'] == 'WORD' or block['BlockType'] == 'LINE':
            text += block.get('Text', '') + '\n'
    
    # Split the text into sections based on the '\n' character
    sections = {}
    current_section = None

    for line in text.split('\n'):
        if line.strip() == "":
            current_section = None
        elif line.isupper():
            current_section = line
            sections[current_section] = []
        elif current_section:
            sections[current_section].append(line)
        else:
            current_section = "PARAGRAPH"
            if current_section not in sections:
                sections[current_section] = []
            sections[current_section].append(line)

    for section, content in sections.items():
        sections[section] = ' '.join(content).strip()

    return sections, text

def extract_information_medical(response):
    patient_info = {}
    doctor_info = {}

    key_map = {}
    value_map = {}
    block_map = {}

    blocks = response.get('Blocks', [])
    if not blocks:
        raise ValueError("No blocks found in Textract response")

    for block in blocks:
        block_id = block['Id']
        block_map[block_id] = block
        if block['BlockType'] == "KEY_VALUE_SET":
            if 'KEY' in block['EntityTypes']:
                key_map[block_id] = block
            else:
                value_map[block_id] = block

    def get_kv_relationship(key_map, value_map, block_map):
        kvs = {}
        for block_id, key_block in key_map.items():
            value_block = None
            for relationship in key_block.get('Relationships', []):
                if relationship['Type'] == 'VALUE':
                    for value_id in relationship['Ids']:
                        value_block = value_map.get(value_id)
                        break
            key = get_text(key_block, block_map)
            value = get_text(value_block, block_map)
            kvs[key] = value
        return kvs

    def get_text(result, blocks_map):
        text = ''
        if result and 'Relationships' in result:
            for relationship in result['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        word = blocks_map[child_id]
                        if word['BlockType'] == 'WORD':
                            text += word['Text'] + ' '
                        if word['BlockType'] == 'SELECTION_ELEMENT':
                            if word['SelectionStatus'] == 'SELECTED':
                                text += 'X '
        return text.strip()

    kvs = get_kv_relationship(key_map, value_map, block_map)

    for key, value in kvs.items():
        if "Mother" in key or "Infant" in key:
            patient_info[key] = value
        elif "Physician" in key or "Doctor" in key:
            doctor_info[key] = value

    # Check for signature presence
    signature_present = False
    for block in blocks:
        if block['BlockType'] == 'SIGNATURE':
            signature_present = True
            break
        if block['BlockType'] == 'LINE' and 'Text' in block and 'signature' in block['Text'].lower():
            signature_present = True
            break

    doctor_info["Physician Signature:"] = "Present" if signature_present else "Not Present"

    return {
        "patient": patient_info,
        "doctor": doctor_info
    }

def extract_information_signed_agreement(response, pdf_text, object_key):
    agreement_info = {
        "Customer/Patient Name": "No name found",
        "Date": "No date found",
        "Signature Present": "No"
    }
    
    # Extract date from filename
    date_match = re.search(r"(\d{4}_\d{2}_\d{2})", object_key)
    if date_match:
        date_str = date_match.group(1).replace('_', '/')
        agreement_info["Date"] = date_str
        print(f"Extracted date from filename: {date_str}")
    
    # Try to extract customer name from the PDF text
    if pdf_text:
        # Save PDF text for manual review
        with open("full_pdf_text.txt", "w", encoding="utf-8") as f:
            f.write(pdf_text)
        
        # Look for "Signed by customer:" pattern
        name_patterns = [
            r"Signed by customer\s*:\s*([^\n\.;,]+)",
            r"Signed by\s*:\s*([^\n\.;,]+)",
            r"Customer\s*:\s*([^\n\.;,]+)"
        ]
        
        for pattern in name_patterns:
            matches = re.findall(pattern, pdf_text, re.IGNORECASE)
            if matches:
                for match in matches:
                    name = match.strip()
                    if len(name) > 2 and len(name) < 50:  # Reasonable name length
                        agreement_info["Customer/Patient Name"] = name
                        print(f"Found customer name from PDF: {name}")
                        break
                if agreement_info["Customer/Patient Name"] != "No name found":
                    break
    
    # If no name found, try once more with a broader approach
    if agreement_info["Customer/Patient Name"] == "No name found" and pdf_text:
        # Find lines containing "signed" or "customer"
        lines = pdf_text.split('\n')
        for i, line in enumerate(lines):
            if ("signed" in line.lower() or "customer" in line.lower()) and ":" in line:
                print(f"Potential signature line: {line}")
                parts = line.split(":", 1)
                if len(parts) > 1:
                    name = parts[1].strip()
                    if name and len(name) > 2:
                        agreement_info["Customer/Patient Name"] = name
                        print(f"Found customer name from line: {name}")
                        break
    
    # Try to extract date from the PDF text
    date_patterns = [
        r"Date\s*:\s*(\d{2}/\d{2}/\d{4})",
        r"Fecha\s*:\s*(\d{2}/\d{2}/\d{4})",
        r"(\d{2}/\d{2}/\d{4})"
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, pdf_text, re.IGNORECASE)
        if matches:
            for match in matches:
                date = match.strip()
                if len(date) == 10:  # Reasonable date length
                    agreement_info["Date"] = date
                    print(f"Found date from PDF: {date}")
                    break
            if agreement_info["Date"] != "No date found":
                break
    
    # Detect signature
    signature_present = detect_signature(response)
    agreement_info["Signature Present"] = "Yes" if signature_present else "No"
    
    return agreement_info

def detect_signature(response):
    try:
        blocks = response.get('Blocks', [])
        
        # Method 1: Check for SIGNATURE block type
        for block in blocks:
            if block.get('BlockType') == 'SIGNATURE':
                print(f"Signature detected via SIGNATURE block type")
                return True
                
        # Method 2: Look for lines that might be signatures
        for block in blocks:
            if block.get('BlockType') == 'LINE':
                if 'Geometry' in block:
                    geometry = block.get('Geometry', {})
                    bbox = geometry.get('BoundingBox', {})
                    width = bbox.get('Width', 0)
                    height = bbox.get('Height', 0)
                    
                    if width > 0.2 and height < 0.05 and 'Text' not in block:
                        print(f"Possible signature detected via geometry analysis")
                        return True
        
        # Method 3: Check for specific text indicators
        all_text = ' '.join([block.get('Text', '').lower() for block in blocks if 'Text' in block])
        signature_indicators = ["signature", "signed", "/s/"]
        for indicator in signature_indicators:
            if indicator in all_text:
                print(f"Signature presence inferred from text: '{indicator}'")
                return True
                
        print("No signatures detected.")
        return False
        
    except Exception as e:
        print(f"Error in signature detection: {e}")
        return False

def determine_document_type(object_key):
    """Determine the type of document based on filename or extension"""
    file_extension = object_key.split('.')[-1].lower()
    
    # Check if this is a medical form (simple rule-based approach)
    if "medical" in object_key.lower() or "health" in object_key.lower():
        return "medical"
    # Check if this is a signed agreement
    elif "agreement" in object_key.lower() or "contract" in object_key.lower() or "sign" in object_key.lower():
        return "agreement"
    # Default determination based on file type
    elif file_extension in ['png', 'jpg', 'jpeg']:
        return "medical"  # Default for images
    elif file_extension == 'pdf':
        return "agreement"  # Default for PDFs
    else:
        return "unknown"

def process_document(bucket_name, object_key):
    """Process document based on its type and format"""
    # Get the file extension
    file_extension = object_key.split('.')[-1].lower()
    document_type = determine_document_type(object_key)
    
    print(f"Processing {document_type} document from {object_key}")
    
    # Get Textract analysis for all document types
    textract_response = textract_extract_text(bucket_name, object_key)
    
    if not textract_response:
        print("Failed to extract text data from document.")
        return None
    
    extracted_info = {}
    
    if document_type == "medical":
        # Process as medical document
        extracted_info = extract_information_medical(textract_response)
        structured_text, full_text = structure_text(textract_response)
        first_line = full_text.split('\n')[0].strip() if full_text else "No Label Found"
        
        # Add the label to the extracted information
        extracted_info = {
            "document label": first_line,
            "data": extracted_info
        }
    
    elif document_type == "agreement":
        # For PDFs, also extract text directly using PyPDF2
        pdf_text = None
        if file_extension == 'pdf':
            pdf_text = extract_text_from_pdf(bucket_name, object_key)
        
        # If it's not a PDF or PDF extraction failed, use text from Textract
        if not pdf_text:
            _, pdf_text = structure_text(textract_response)
        
        # Process as agreement document
        extracted_info = extract_information_signed_agreement(textract_response, pdf_text, object_key)
    
    else:
        print(f"Unknown document type for {object_key}")
        return None
    
    return extracted_info

def main(bucket_name, entity_tag_to_find, index=None):
    object_key = None

    # Try to retrieve the document by array index if index is not None
    if index is not None:
        object_key = get_s3_bucket_object_by_index(bucket_name, index)
    
    # If no object key is found by index or index is None, fallback to entity tag
    if not object_key:
        print(f"Object not found by index or index is None, searching by entity tag '{entity_tag_to_find}'")
        object_key = get_s3_bucket_object_by_tag(bucket_name, entity_tag_to_find)

    if object_key:
        print(f"Found document - Object Key: {object_key}")
        
        # Process the document and get extracted information
        extracted_info = process_document(bucket_name, object_key)
        
        if extracted_info:
            # Convert the extracted information to JSON format
            json_output = json.dumps(extracted_info, indent=4)
            print(f"Extracted Information in JSON format:\n{json_output}")
            
            # Save the JSON output to a file
            with open("extracted_info.json", "w") as json_file:
                json_file.write(json_output)
        else:
            print("Failed to extract information from document.")
    else:
        print(f"No document found with the specified index or entity tag in the S3 bucket.")

if __name__ == "__main__":
    bucket_name = "capstone-intelligent-document-processing"
    
    # Use the entity tag defined at the top of the file
    main(bucket_name, ENTITY_TAG, index=index_1)