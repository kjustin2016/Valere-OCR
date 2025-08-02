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
        # Skip any fields containing ICD codes or Z39.1
        if "icd" in key_lower or "z39" in key_lower or "lactating" in key_lower:
            continue
            
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
    
    # 1. Remove unwanted fields completely
    unwanted_fields = [
        "Mother expects regular separation from infant",
        "Mother expects regular",
        "Care of the lactating mother",
        "Z39.1",
        "ICD-10"
    ]
    
    for field_pattern in unwanted_fields:
        for key in list(patient_info.keys()):
            if field_pattern in key:
                del patient_info[key]
    
    # 2. Move physician info from patient to doctor section
    for key in list(patient_info.keys()):
        if "physician" in key.lower() or "doctor" in key.lower():
            doctor_value = patient_info[key]
            if "Doctor Name" not in doctor_info:
                doctor_info["Doctor Name"] = doctor_value
            del patient_info[key]
    
    # 3. Reorganize infant information into patient section
    infant_info = {}  # Temporary holder for infant data
    
    # Extract infant data from patient section
    for key in list(patient_info.keys()):
        if "infant" in key.lower() or "baby" in key.lower():
            value = patient_info[key]
            clean_key = key.replace("Infant ", "").replace("Baby ", "")
            infant_info[clean_key] = value
            del patient_info[key]
    
    # Add infant data to patient section with clear labeling
    if infant_info:
        for key, value in infant_info.items():
            patient_info[f"Infant {key}"] = value
    
    # 4. Fix standard field names and apply cleaning rules
    field_preferences = {
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
    
    # 5. Clean up the X values in doctor_info
    for key, value in list(doctor_info.items()):
        if value == "X":
            # Replace X with "Selected" for clarity
            doctor_info[key] = "Selected"
    
    # 6. Fix MD signature if it's just a single character (likely misread)
    if "MD Signature" in doctor_info and len(doctor_info["MD Signature"]) <= 1:
        doctor_info["MD Signature"] = "Present"
    
    # 7. Remove MD Signature field as it's redundant with Physician Signature
    if "MD Signature" in doctor_info and "Physician Signature" in doctor_info:
        del doctor_info["MD Signature"]
    
    # 8. Extract Doctor Name and NPI from combined fields
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

    # 9. Split Patient Name into First Name and Last Name with handling for "Last, First" format
    if "Patient Name" in patient_info:
        full_name = patient_info["Patient Name"].strip()
        
        # Check if name is in "Last, First" format
        if "," in full_name:
            # Split by comma and handle "Last, First" format
            parts = full_name.split(",", 1)
            if len(parts) == 2:
                last_name = parts[0].strip()
                first_name = parts[1].strip()
                patient_info["First Name"] = first_name
                patient_info["Last Name"] = last_name
                del patient_info["Patient Name"]
        else:
            # Handle normal "First Last" format
            name_parts = full_name.split()
            if len(name_parts) >= 2:
                patient_info["First Name"] = name_parts[0]
                patient_info["Last Name"] = ' '.join(name_parts[1:])
                del patient_info["Patient Name"]
            elif len(name_parts) == 1:
                patient_info["First Name"] = name_parts[0]
                patient_info["Last Name"] = ""
                del patient_info["Patient Name"]
    
    # 10. Also check existing First/Last name for comma pattern
    if "First Name" in patient_info and "Last Name" in patient_info:
        first_name = patient_info["First Name"]
        last_name = patient_info["Last Name"]
        
        # If first name ends with comma, it's likely "Last, First" format
        if first_name.endswith(","):
            # Swap First and Last name
            patient_info["First Name"] = last_name
            patient_info["Last Name"] = first_name.rstrip(",")
            
    # 11. Rename "Selected" to more descriptive "Is Prescribing Physician"
    if "Selected" in doctor_info:
        doctor_info["Is Prescribing Physician"] = doctor_info["Selected"]
        del doctor_info["Selected"]

    # 12. Replace OCR-interpreted signature text with "Present"
    if "Physician Signature" in doctor_info:
        doctor_info["Physician Signature"] = "Present"
        
    # 13. Fix issue with "Infant Name" containing "Infant Date of Birth"
    if "Infant Name" in patient_info and patient_info["Infant Name"] == "Infant Date of Birth":
        patient_info["Infant Name"] = "Not present"
    
    # Return the cleaned data
    return {
        "patient": patient_info,
        "doctor": doctor_info,
        "prescription": prescription_info
    }

def extract_information_signed_agreement(response, pdf_text, object_key):
    # Initialize with default "Not present" values
    agreement_info = {
        "Customer/Patient Name": "Not present",
        "Date": "Not present",
        "Signature Present": "No"
    }
    
    # Try to extract date from the PDF text only (not from filename)
    if pdf_text:
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
                if agreement_info["Date"] != "Not present":
                    break
    
    # Try to extract customer name from the PDF text
    if pdf_text:
        # Common name patterns
        name_patterns = [
            r"Signed by customer\s*:\s*([^\n\.;,]+)",
            r"Signed by\s*:\s*([^\n\.;,]+)",
            r"Customer\s*:\s*([^\n\.;,]+)",
            r"Patient\s*:\s*([^\n\.;,]+)",
            r"Name\s*:\s*([^\n\.;,]+)"
        ]
        
        # List of phrases to exclude as false positives
        excluded_phrases = [
            "to be", "the ", "please", "notify", "customer rights", 
            "submit", "have the right", "fully informed",
            "contact", "patient's", "if you", "please", "thank you"
        ]
        
        for pattern in name_patterns:
            matches = re.findall(pattern, pdf_text, re.IGNORECASE)
            if matches:
                for match in matches:
                    name = match.strip()
                    # More stringent name validation
                    if (len(name) > 2 and len(name) < 50 and 
                        not any(phrase in name.lower() for phrase in excluded_phrases)):
                        agreement_info["Customer/Patient Name"] = name
                        print(f"Found customer name from PDF: {name}")
                        break
                if agreement_info["Customer/Patient Name"] != "Not present":
                    break
    
    # Detect signature
    signature_present = detect_signature(response)
    agreement_info["Signature Present"] = "Yes" if signature_present else "No"
    
    # Return the simple, flat structure matching the example
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

def list_all_s3_objects(bucket):
    """List all objects in an S3 bucket"""
    all_objects = []
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)
    
    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            all_objects.extend(page['Contents'])
    
    return all_objects

def check_document_content(bucket_name, object_key):
    """Quick check to determine if a document is likely a prescription or agreement"""
    try:
        # Check filename for explicit type indicators
        lower_key = object_key.lower()
        
        # These are strong indicators directly from the filename
        if "insurancecard" in lower_key:
            print(f"Filename indicates this is an insurance card")
            return "INSURANCE_CARD", 10
        elif "facesheet" in lower_key:
            print(f"Filename indicates this is a face sheet")
            return "FACE_SHEET", 10
        elif "signed_agreement" in lower_key or ("agreement" in lower_key and not "prescription" in lower_key):
            print(f"Filename indicates this is an agreement document")
            return "SIGNED_AGREEMENT", 10
        elif "prescription" in lower_key and not "agreement" in lower_key:
            print(f"Filename indicates this is a prescription document")
            return "PRESCRIPTION", 10
        elif "id" in lower_key and "card" in lower_key:
            print(f"Filename indicates this is an ID document")
            return "ID_DOCUMENT", 10
        
        # If no strong filename indicators, analyze content
        # Get the file extension
        file_extension = object_key.split('.')[-1].lower()
        
        # Extract text using the appropriate method
        full_text = ""
        
        # For PDFs, use PyPDF2 first as it's faster
        if file_extension == 'pdf':
            pdf_text = extract_text_from_pdf(bucket_name, object_key)
            if pdf_text:
                full_text = pdf_text
        
        # If we don't have text yet or for non-PDF files, use Textract
        if not full_text:
            textract_response = textract_extract_text(bucket_name, object_key)
            if textract_response:
                _, extracted_text = structure_text(textract_response)
                full_text = extracted_text
        
        # Convert to lowercase for case-insensitive matching
        lower_text = full_text.lower()
        
        # Define keywords for each document type
        prescription_keywords = [
            "prescription", "rx", "physician", "doctor", "diagnosis", 
            "patient name", "mother name", "mother's name", "breast pump", 
            "icd-10", "medical necessity", "dob", "date of birth"
        ]
        
        agreement_keywords = [
            "agreement", "signature", "signed", "consent", "terms", 
            "conditions", "i agree", "customer", "acknowledge"
        ]
        
        insurance_keywords = [
            "insurance", "member", "policy", "group", "copay", "deductible",
            "plan", "coverage", "id#", "id #", "insured", "subscriber"
        ]
        
        # Count keywords for each type
        prescription_count = sum(1 for kw in prescription_keywords if kw in lower_text)
        agreement_count = sum(1 for kw in agreement_keywords if kw in lower_text)
        insurance_count = sum(1 for kw in insurance_keywords if kw in lower_text)
        
        # Document is classified based on which type has more keyword matches
        if insurance_count >= 2:
            # Insurance cards take precedence to avoid misclassification
            return "INSURANCE_CARD", insurance_count
        elif prescription_count >= 3 and prescription_count > agreement_count:
            return "PRESCRIPTION", prescription_count
        elif agreement_count >= 3 and agreement_count >= prescription_count:
            return "SIGNED_AGREEMENT", agreement_count
        elif prescription_count >= 2:
            return "POSSIBLE_PRESCRIPTION", prescription_count
        elif agreement_count >= 2:
            return "POSSIBLE_AGREEMENT", agreement_count
        else:
            return "UNKNOWN", 0
            
    except Exception as e:
        print(f"Error checking document content for {object_key}: {str(e)}")
        return "ERROR", 0

def process_selected_documents(bucket_name):
    """Process only prescriptions and signed agreements from the bucket"""
    all_objects = list_all_s3_objects(bucket_name)
    print(f"Found {len(all_objects)} total objects in bucket {bucket_name}")
    
    prescription_count = 0
    agreement_count = 0
    skipped_count = 0
    
    # Create output directory if it doesn't exist
    output_dir = "extracted_documents"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Process objects
    for i, obj in enumerate(all_objects):
        object_key = obj['Key']
        file_extension = object_key.split('.')[-1].lower()
        
        # Skip unsupported file types
        if file_extension not in ['pdf', 'jpg', 'jpeg', 'png']:
            skipped_count += 1
            print(f"Skipping unsupported file type: {object_key}")
            continue
        
        print(f"[{i+1}/{len(all_objects)}] Checking: {object_key}")
        
        # Determine if this document is likely a prescription or signed agreement
        doc_type, confidence = check_document_content(bucket_name, object_key)
        
        if "PRESCRIPTION" in doc_type:
            print(f"Processing prescription document: {object_key}")
            try:
                # Extract detailed information
                extracted_info = process_document(bucket_name, object_key)
                
                if extracted_info:
                    # Save to individual JSON file
                    prescription_count += 1
                    output_filename = f"{output_dir}/prescription_extract_{prescription_count}.json"
                    
                    result = {
                        "object_key": object_key,
                        "document_type": "PRESCRIPTION",
                        "extracted_data": extracted_info
                    }
                    
                    with open(output_filename, "w") as json_file:
                        json.dump(result, json_file, indent=4)
                    
                    print(f"Saved prescription data to {output_filename}")
            except Exception as e:
                print(f"Error processing prescription document {object_key}: {str(e)}")
                
        elif "AGREEMENT" in doc_type:
            print(f"Processing agreement document: {object_key}")
            try:
                # Extract agreement information
                pdf_text = None
                if file_extension == 'pdf':
                    pdf_text = extract_text_from_pdf(bucket_name, object_key)
                
                textract_response = textract_extract_text(bucket_name, object_key)
                
                if textract_response:
                    agreement_info = extract_information_signed_agreement(textract_response, pdf_text, object_key)
                    
                    # Save to individual JSON file
                    agreement_count += 1
                    output_filename = f"{output_dir}/signed_agreement_extract_{agreement_count}.json"
                    
                    result = {
                        "object_key": object_key,
                        "document_type": "SIGNED_AGREEMENT",
                        "extracted_data": agreement_info
                    }
                    
                    with open(output_filename, "w") as json_file:
                        json.dump(result, json_file, indent=4)
                    
                    print(f"Saved agreement data to {output_filename}")
            except Exception as e:
                print(f"Error processing agreement document {object_key}: {str(e)}")
        else:
            skipped_count += 1
    
    print(f"Processing complete!")
    print(f"Processed {prescription_count} prescription documents")
    print(f"Processed {agreement_count} signed agreement documents")
    print(f"Skipped {skipped_count} documents")
    
    # Create a summary file
    summary = {
        "total_documents": len(all_objects),
        "processed_prescriptions": prescription_count,
        "processed_agreements": agreement_count,
        "skipped_documents": skipped_count
    }
    
    with open(f"{output_dir}/processing_summary.json", "w") as summary_file:
        json.dump(summary, summary_file, indent=4)

def process_document(bucket_name, object_key):
    """Process document based on its type and format"""
    # Get the file extension
    file_extension = object_key.split('.')[-1].lower()
    
    # Get Textract analysis
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
    
    first_line = full_text.split('\n')[0].strip() if full_text else "No Label Found"
    
    # Determine document type based on content
    lower_text = full_text.lower()
    
    # Check for prescription keywords
    prescription_keywords = ["prescription", "breast pump", "mother", "infant", "physician", "doctor", "medical necessity"]
    agreement_keywords = ["agreement", "signed", "consent", "signature", "acknowledge"]
    
    # Count keyword matches
    prescription_score = sum(1 for kw in prescription_keywords if kw in lower_text)
    agreement_score = sum(1 for kw in agreement_keywords if kw in lower_text)
    
    extracted_info = {}
    
    # Process as appropriate type based on content
    if prescription_score > agreement_score:
        # Process as medical document with pdf_text when available
        medical_info = extract_information_medical(textract_response, pdf_text)
        
        # Add the label to the extracted information
        extracted_info = {
            "document_label": first_line,
            "data": {
                "patient": medical_info["patient"],
                "doctor": medical_info["doctor"]
            }
        }
        
        # Additional cleanup for Breast Pump Depot format
        if "The Breast Pump Depot" in first_line:
            patient_info = extracted_info["data"]["patient"]
            doctor_info = extracted_info["data"]["doctor"]
            
            # Move physician name to doctor section if it's in patient section
            if "Physician Name" in patient_info:
                doctor_info["Doctor Name"] = patient_info["Physician Name"]
                del patient_info["Physician Name"]
            
            # Move Physician NPI to standardized format
            if "Physician NPI" in doctor_info:
                doctor_info["NPI"] = doctor_info["Physician NPI"]
                del doctor_info["Physician NPI"]
            
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
    
    else:
        # Process as agreement document
        extracted_info = extract_information_signed_agreement(textract_response, pdf_text, object_key)
    
    # Remove infant section and consolidate with patient data if it exists
    if "data" in extracted_info and isinstance(extracted_info["data"], dict) and "infant" in extracted_info["data"]:
        infant_data = extracted_info["data"].pop("infant")
        # Add infant data to patient section with clear labeling
        for key, value in infant_data.items():
            extracted_info["data"]["patient"][f"Infant {key}"] = value
    
    return extracted_info

def main(bucket_name):
    import os
    print("Files are being saved to:", os.getcwd())
    process_selected_documents(bucket_name)

if __name__ == "__main__":
    bucket_name = "capstone-intelligent-document-processing"
    main(bucket_name)