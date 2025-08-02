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
ENTITY_TAG = "bf3f6699401b86622f17161754d168e5" # Replace with the desired entity tag
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

def extract_information_medical(response, pdf_text=None):
    patient_info = {}
    doctor_info = {}
    prescription_info = {}

    key_map = {}
    value_map = {}
    block_map = {}

    blocks = response.get('Blocks', [])
    if not blocks:
        raise ValueError("No blocks found in Textract response")

    # Create block maps for KV processing
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

    # Extract text from all LINE blocks to help with pattern matching
    all_lines = []
    for block in blocks:
        if block['BlockType'] == 'LINE' and 'Text' in block:
            all_lines.append(block['Text'])
    all_text = "\n".join(all_lines)

    # Use PyPDF2 extracted text if available (helpful for certain PDF formats)
    if pdf_text:
        print("Using PyPDF2 extracted text for additional analysis")
        all_text += "\n" + pdf_text

    # Get Key-Value pairs
    kvs = get_kv_relationship(key_map, value_map, block_map)

    # Check for document format - Texas Children's Hospital or Breast Pump Depot
    is_texas_childrens = "Texas Children's Hospital" in all_text
    is_breast_pump_depot = "The Breast Pump Depot" in all_text
    
    # Process key-value pairs for patient and doctor information
    for key, value in kvs.items():
        key_lower = key.lower()
        if any(term in key_lower for term in ["mother", "infant", "patient", "name", "dob", "date of birth", "phone"]):
            patient_info[key] = value
        elif any(term in key_lower for term in ["physician", "doctor", "md", "prescribing"]):
            doctor_info[key] = value

    # Extract patient information through pattern matching
    # Common patterns for patient details
    patterns = [
        (r"(?:Mother|Patient)\s*Name[:\s]+([^:\n]+)", "Patient Name"),
        (r"(?:Mother|Patient)?\s*Date of [Bb]irth[:\s]+([^:\n]+)", "Date of Birth"),
        (r"(?:Mother|Patient)?\s*DOB[:\s]+([^:\n]+)", "DOB"),
        (r"(?:Mother|Patient)?\s*Phone\s*(?:Number)?[:\s]+([^:\n]+)", "Phone Number"),
        (r"(?:Infant|Baby)\s*Name[:\s]+([^:\n]+)", "Infant Name"),
        (r"(?:Infant|Baby)\s*Date of [Bb]irth[:\s]+([^:\n]+)", "Infant Date of Birth"),
        (r"EDD[:\s]+([^:\n]+)", "EDD")
    ]

    # Apply all patterns to all_text
    for pattern, field_name in patterns:
        matches = re.findall(pattern, all_text, re.IGNORECASE)
        if matches:
            patient_info[field_name] = matches[0].strip()
    
    # PRESCRIPTION INFORMATION EXTRACTION - FORMAT SPECIFIC
    if is_texas_childrens:
        # Texas Children's Hospital format extraction
        # Extract the ICD-10 code for Texas Children's
        icd_match = re.search(r"ICD-10 Code\(?s?\)?.*?[•❖★✦●■▪]\s*([A-Z]\d+\.\d+)", all_text, re.IGNORECASE | re.DOTALL)
        if icd_match:
            prescription_info["ICD-10 Code"] = icd_match.group(1).strip()
        else:
            # Try another pattern without bullet points
            icd_match = re.search(r"ICD-10 Code\(?s?\)?[:\s]*([A-Z]\d+\.\d+)", all_text, re.IGNORECASE)
            if icd_match:
                prescription_info["ICD-10 Code"] = icd_match.group(1).strip()
        
        # Look for specific breast pump prescription for Texas Children's
        pump_match = re.search(r"One \(1\) double-electric breast pump", all_text, re.IGNORECASE)
        if pump_match:
            prescription_info["Prescription"] = "One (1) double-electric breast pump"
        
        # Look for specific checked items
        checkbox_lines = [line for line in all_lines if "✓" in line or "X" in line or "•" in line]
        for line in checkbox_lines:
            if "Z39.1" in line:
                prescription_info["ICD-10 Code"] = "Z39.1"
    
    elif is_breast_pump_depot:
        # The Breast Pump Depot format extraction
        # Look for the prescription section
        item_match = re.search(r"Item Description\s*Code\s*Length of Need", all_text, re.IGNORECASE)
        if item_match:
            # Try to extract details under the Item Description section
            qty_match = re.search(r"QTY\s*(\d+)\s*([^C]+)\s*Code\s*(\w+)", all_text, re.IGNORECASE)
            if qty_match:
                qty = qty_match.group(1).strip()
                item = qty_match.group(2).strip()
                code = qty_match.group(3).strip()
                prescription_info["Quantity"] = qty
                prescription_info["Item"] = item
                prescription_info["Code"] = code
            
            # Look specifically for breast pump items
            pump_pattern = r"Double Electric Breast Pump"
            if re.search(pump_pattern, all_text, re.IGNORECASE):
                prescription_info["Prescription"] = "Double Electric Breast Pump"
            
            # Extract length of need if present
            need_match = re.search(r"Length of Need:?\s*(\d+)", all_text, re.IGNORECASE)
            if need_match:
                prescription_info["Length of Need"] = need_match.group(1).strip() + " months"
        
        # Look for medical necessity codes in Section II
        med_necessity_match = re.search(r"Section II. Medical Necessity(.*?)Section III", all_text, re.IGNORECASE | re.DOTALL)
        if med_necessity_match:
            necessity_text = med_necessity_match.group(1)
            # Look for checked items
            checkbox_items = re.findall(r"[✓X]\s*\d+\.\s*([^\n]+)", necessity_text)
            if checkbox_items:
                prescription_info["Medical Necessity"] = checkbox_items
    else:
        # Generic prescription information extraction for unknown formats
        # Look for ICD codes in any format
        icd_codes = re.findall(r"[A-Z]\d+\.\d+", all_text)
        if icd_codes:
            prescription_info["ICD-10 Code"] = icd_codes[0]
        
        # Look for breast pump mentions
        pump_patterns = [
            r"breast pump", 
            r"double electric", 
            r"double-electric",
            r"electric breast pump"
        ]
        
        for pattern in pump_patterns:
            if re.search(pattern, all_text, re.IGNORECASE):
                prescription_info["Prescription"] = "Double Electric Breast Pump"
                break

    # Clean up data by removing duplicates and empty values
    patient_info = {k.replace(":", "").strip(): v for k, v in patient_info.items() if v and v.strip()}
    doctor_info = {k.replace(":", "").strip(): v for k, v in doctor_info.items() if v and v.strip()}
    prescription_info = {k.replace(":", "").strip(): v for k, v in prescription_info.items() if v and v.strip()}

    # ADDITIONAL CLEANUP BEFORE RETURNING
    
    # 1. Remove duplicate fields with different names (prefer standard names)
    field_preferences = {
        # Standard name: [aliases to remove if standard exists]
        "Patient Name": ["Name"],
        "DOB": ["Date of Birth"],
        "Phone Number": ["Phone"]
    }
    
    for standard, aliases in field_preferences.items():
        if standard in patient_info:
            # Remove aliases if the standard field exists
            for alias in aliases:
                if alias in patient_info:
                    del patient_info[alias]
    
    # 2. Clean up the X values in doctor_info and prescription_info
    for section in [doctor_info, prescription_info]:
        for key, value in list(section.items()):
            if value == "X":
                # Replace X with "Selected" for clarity
                section[key] = "Selected"
    
    # 3. Fix MD signature if it's just a single character (likely misread)
    if "MD Signature" in doctor_info and len(doctor_info["MD Signature"]) <= 1:
        doctor_info["MD Signature"] = "Present"
    
    # 4. Remove MD Signature field as it's redundant with Physician Signature
    if "MD Signature" in doctor_info and "Physician Signature" in doctor_info:
        del doctor_info["MD Signature"]
    
    # 5. Extract Doctor Name and NPI from combined fields
    for key, value in list(doctor_info.items()):
        # Look for pattern like "Doctor Name, MD - NPI_NUMBER"
        doctor_npi_match = re.search(r"(.+?),?\s+MD\s+-\s+(\d+)", key)
        if doctor_npi_match:
            doctor_name = doctor_npi_match.group(1).strip()
            npi_number = doctor_npi_match.group(2).strip()
            
            # Add new fields
            doctor_info["Doctor Name"] = doctor_name
            doctor_info["NPI"] = npi_number
            
            # If the original field was selected, note that
            if value == "Selected":
                doctor_info["Selected"] = "Yes"
                
            # Remove the original combined field
            del doctor_info[key]

    # 6. Split Patient Name into First Name and Last Name
    if "Patient Name" in patient_info:
        full_name = patient_info["Patient Name"].strip()
        name_parts = full_name.split()
        
        if len(name_parts) >= 2:
            patient_info["First Name"] = name_parts[0]
            patient_info["Last Name"] = ' '.join(name_parts[1:])
            del patient_info["Patient Name"]  # Remove the original full name
        elif len(name_parts) == 1:
            patient_info["First Name"] = name_parts[0]
            patient_info["Last Name"] = ""
            del patient_info["Patient Name"]

    # 7. Rename "Selected" to more descriptive "Is Prescribing Physician"
    if "Selected" in doctor_info:
        doctor_info["Is Prescribing Physician"] = doctor_info["Selected"]
        del doctor_info["Selected"]

    # Add this in the cleanup section of your function
    if "Physician Signature" in doctor_info:
        # Replace OCR-interpreted signature text with "Present"
        doctor_info["Physician Signature"] = "Present"
    
    return {
        "patient": patient_info,
        "doctor": doctor_info,
        "prescription": prescription_info
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
    
    # Check keywords in the filename
    lower_object_key = object_key.lower()
    
    # Medical document keywords
    medical_keywords = ["medical", "health", "patient", "hospital", "doctor", 
                        "prescription", "pump", "breast", "physician", "clinic"]
    
    # Agreement keywords
    agreement_keywords = ["agreement", "contract", "sign", "signed", "consent", "form"]
    
    # Check for medical keywords
    for keyword in medical_keywords:
        if keyword in lower_object_key:
            return "medical"
    
    # Check for agreement keywords
    for keyword in agreement_keywords:
        if keyword in lower_object_key:
            return "agreement"
    
    # Default determination based on file type
    if file_extension in ['png', 'jpg', 'jpeg']:
        return "medical"  # Default for images
    elif file_extension == 'pdf':
        # For PDFs, we'll try to look at the content to decide
        return "unknown"  # We'll determine based on content later
    else:
        return "unknown"
    
def refine_document_type(document_type, extracted_text):
    """Refine the document type based on the content"""
    lower_text = extracted_text.lower()
    
    # Check for medical indicators
    medical_indicators = ["patient", "doctor", "hospital", "clinic", "prescription", 
                          "medical", "health", "physician", "diagnosis"]
    
    # Check for agreement indicators
    agreement_indicators = ["agreement", "contract", "consent", "sign", "signature", 
                           "terms", "conditions", "acknowledge"]
    
    medical_score = sum(1 for indicator in medical_indicators if indicator in lower_text)
    agreement_score = sum(1 for indicator in agreement_indicators if indicator in lower_text)
    
    if medical_score > agreement_score:
        return "medical"
    elif agreement_score > medical_score:
        return "agreement"
    else:
        return document_type  # Keep the original type if we're not sure

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
    
    # For PDFs, also extract text directly using PyPDF2
    pdf_text = None
    if file_extension == 'pdf':
        pdf_text = extract_text_from_pdf(bucket_name, object_key)
    
    # Get structured text
    structured_text, full_text = structure_text(textract_response)
    
    # Refine document type based on content if it was uncertain
    if document_type == "unknown":
        document_type = refine_document_type(document_type, full_text)
        print(f"Refined document type: {document_type}")
    
    first_line = full_text.split('\n')[0].strip() if full_text else "No Label Found"
    
    extracted_info = {}
    
    if document_type == "medical":
        # Process as medical document with pdf_text when available
        medical_info = extract_information_medical(textract_response, pdf_text)
        
        # Add the label to the extracted information
        extracted_info = {
            "document label": first_line,
            "data": medical_info
        }
        
        # Additional cleanup for Breast Pump Depot format
        if "The Breast Pump Depot" in first_line:
            data = extracted_info["data"]
            patient_info = data["patient"]
            doctor_info = data["doctor"] 
            prescription_info = data["prescription"]
            
            # Move physician name to doctor section if it's in patient section
            if "Physician Name" in patient_info:
                doctor_info["Doctor Name"] = patient_info["Physician Name"]
                del patient_info["Physician Name"]
            
            # Move Physician NPI to standardized format
            if "Physician NPI" in doctor_info:
                doctor_info["NPI"] = doctor_info["Physician NPI"]
                del doctor_info["Physician NPI"]
            
            # Clean up prescription checkboxes for breast pump depot format
            for key, value in list(patient_info.items()):
                if "Z39.1" in key and (value == "X" or value == "Selected"):
                    # Move this to prescription section
                    prescription_info["ICD-10 Code"] = "Z39.1"
                    prescription_info["Diagnosis"] = key.replace("(Z39.1)", "").strip()
                    del patient_info[key]
            
            # Remove duplicate fields in patient info
            if "First Name" in patient_info and "Last Name" in patient_info:
                # If we already have First/Last name, remove Mother Name
                if "Mother Name" in patient_info:
                    del patient_info["Mother Name"]
            
            # Handle date of birth fields - convert all to standard DOB format
            if "Date of Birth" in patient_info or "Mother Date of Birth" in patient_info:
                # Get DOB from whichever field is available
                dob_value = patient_info.get("Date of Birth") or patient_info.get("Mother Date of Birth")
    
                # Set the standard DOB field and remove others
                patient_info["DOB"] = dob_value
    
                # Clean up duplicate date fields
                if "Date of Birth" in patient_info:
                    del patient_info["Date of Birth"]
                if "Mother Date of Birth" in patient_info:
                    del patient_info["Mother Date of Birth"]
            
            if "Phone Number" in patient_info and "Mother Phone Number" in patient_info:
                # If both exist, prefer the standard one
                if patient_info["Phone Number"] == patient_info["Mother Phone Number"]:
                    del patient_info["Mother Phone Number"]
            
            # Handle infant information
            if "Infant Name" in patient_info:
                # Create dedicated infant section
                data["infant"] = {}
                
                data["infant"]["Name"] = patient_info["Infant Name"]
                del patient_info["Infant Name"]
                
                if "Infant Date of Birth" in patient_info:
                    data["infant"]["DOB"] = patient_info["Infant Date of Birth"]
                    del patient_info["Infant Date of Birth"]
    
    elif document_type == "agreement":
        # Process as agreement document
        extracted_info = extract_information_signed_agreement(textract_response, pdf_text, object_key)
    
    else:
        # If still unknown, try processing as medical document as fallback
        print(f"Document type still unknown, trying as medical document")
        extracted_info = extract_information_medical(textract_response, pdf_text)
        extracted_info = {
            "document label": first_line,
            "data": extracted_info
        }
    
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
