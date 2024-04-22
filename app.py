from pdf2image import convert_from_bytes
from pytesseract import image_to_string
import re
import pycountry
import os
import pyodbc
from azure.storage.blob import BlobServiceClient
from datetime import datetime

# Regular expressions for extracting information
eh_reference_regex = r'\bEH reference number\s*:\s*(\d{10})\b'
euler_regex = r'\bEuler N°\s*:\s*(\d{10})\b'
telephone_regex = r'Telephone\s*:\s*\((\d{3})\) (\d{1,2} \d{3} \d{4})'

# Function to extract information from a file text
def extract_info_from_file(file_text):
    mastertext = file_text.strip()
    st = [line for line in mastertext.split('\n') if line.strip()]
    
    # Extract Speed of Report in Text
    speed = get_next_index(st, 'EH reference number')
    
    # Extract telephone number
    telephone_number = get_telephone(st, 'Telephone :') 
    
    # Extract company name
    company_name = get_req_company_name(st, 'We would like to receive information about the company below')
    
    # Extract Euler number
    euler_number = re.search(euler_regex, file_text)
    euler_number = euler_number.group(1) if euler_number else None
    
    # Extract date
    date_regex = r'\b(\d{1,2}(?:st|nd|rd|th)?(?:\s+\w+)?(?:\s+\d{4})?)\b'
    date_match = re.search(date_regex, file_text)
    date = date_match.group(1).strip() if date_match else None
    
    # Extract address
    address = get_address(file_text)
    
    # Extract reports
    reports = get_reports(file_text, telephone_number, address)
    
    # Process speed
    speed = process_speed(speed)

    reg_number = get_reg_number(address)

    return euler_number, telephone_number, address, date, speed, company_name, reg_number, reports

def connect_to_database():
    try:
        # Replace connection string parameters with your actual database credentials
        server = 'localhost'
        database = 'DemoDB'
        username = 'sa'
        password = 'sa123'
        driver = '{SQL Server}'

        # Construct the connection string
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        return None
    
# Function to extract telephone number
def get_telephone(lst, target_string):
    for line in lst:
        if target_string in line:
            telephone_index = line.find(":")
            if telephone_index != -1:
                return line[telephone_index + 1:].strip()
    return None

# Function to extract required company name
def get_req_company_name(lst, target_string):
    for i, line in enumerate(lst):
        if target_string in line:
             return lst[i+2] if i+2 < len(lst) else None
    return None

# Function to extract address
def get_address(text):
    address_regex = r"We would like to receive information about the company below:(.*?)(?=Telephone|$)"
    address_match = re.search(address_regex, text, re.DOTALL)
    address = address_match.group(1).strip() if address_match else None
    return address

# Function to extract reports
def get_reports(text, telephone_number, address):
    if telephone_number:
        end_of_telephone_index = text.lower().find(telephone_number.lower()) + len(telephone_number)
        if "yours faithfully" in text.lower():
            end_index = text.lower().find("yours faithfully", end_of_telephone_index)
            if end_index != -1:
                return text[end_of_telephone_index:end_index].strip()
        else:
            return text[end_of_telephone_index:].strip()
    else:
        if address:
            address_end_index = text.find(address) + len(address)
            if "yours faithfully" in text.lower():
                end_index = text.lower().find("yours faithfully", address_end_index)
                if end_index != -1:
                    return text[address_end_index:end_index].strip()
            else:
                return text[address_end_index:].strip()
    return None

# Function to process speed
def process_speed(speed):
    if speed:
        speed_str = str(speed).lower()
        try:
            speed_int = int(speed)
            if speed_int in [3, 5] or speed_str == "express":
                return "Express"
            elif speed_int == 10 or speed_str in ["revision", "normal"]:
                return "Normal"
        except ValueError:
            if speed_str in ["3", "5", "express"]:
                return "Express"
            elif speed_str in ["10", "revision", "normal"]:
                return "Normal"
    return None

def get_reg_number(address):
    first_line_tokens = address.split('\n')[0].split()
    first_line_has_number = any(any(char.isdigit() for char in token) for token in first_line_tokens)
    first_line_has_combination = any(
        all(
            (char.isalnum() or char in '-: /')
            for char in token
        ) and (any(char.isalpha() for char in token) and any(not char.isalnum() and not char.isdigit() for char in token))
        for token in first_line_tokens
    )

    reg_number = None

    if first_line_has_number or first_line_has_combination:
        first_line = address.split('\n')[0].strip()
        colon_index = first_line.find(':')
        if colon_index != -1:
            reg_number = first_line[colon_index + 1:].strip()
    else:
        # If no number or combination is present on the first line, consider the first line as the company name
        reg_number = None

    return reg_number

# Function to convert PDF bytes to text
def convert_pdf_to_text(pdf_bytes):
    images = convert_from_bytes(pdf_bytes)
    text = ""
    for img in images:
        text += image_to_string(img)
    return text

# Function to get PDF bytes from Azure Blob Storage
def get_pdf_bytes(blob_service_client, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    pdf_bytes = blob_client.download_blob().readall()
    return pdf_bytes

def convert_date_string(date_str):
    # Remove the ordinal indicator ("rd") from the day part of the date string
    date_str = date_str.replace('rd', '').replace('th', '').replace('st', '').replace('nd', '')

    # Parse the modified date string into a datetime object
    date_obj = datetime.strptime(date_str, "%d %B %Y")

    # Format the datetime object into the desired format
    formatted_date = date_obj.strftime("%d/%m/%Y")

    return formatted_date

def process_pdf_files():
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not connect_str:
        print("Azure Storage connection string not found. Set the AZURE_STORAGE_CONNECTION_STRING environment variable.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = 'dbeditor'

    unextracted_pdf_names = get_unextracted_pdf_names_from_database()
    pdf_files = []

    for pdf_name in unextracted_pdf_names:
        for blob in blob_service_client.get_container_client(container_name).list_blobs():
            if blob.name.endswith('.pdf') and pdf_name in blob.name:
                pdf_bytes = get_pdf_bytes(blob_service_client, container_name, blob.name)
                euler_number, telephone_number, address, date, speed, company_name, reg_number, reports = extract_info_from_file(convert_pdf_to_text(pdf_bytes))

                company_index = address.lower().find(company_name.lower())
                if company_index != -1:
                    address = address[company_index + len(company_name):]  

                yours_faithfully_index = address.lower().find("yours faithfully")
                if yours_faithfully_index != -1:
                    address = address[:yours_faithfully_index]  

                address_combined = " ".join(line.strip() for line in address.split('\n') if line.strip())

                telephone_value = telephone_number if telephone_number else "Telephone: None"

                country = countryfind(address.split('\n'))
                formatted_date = convert_date_string(date)

                pdf_data = {
                    "date": formatted_date,
                    "client_ref": euler_number,
                    "company_name": company_name,
                    "address": address_combined,
                    "country": country,
                    "telephone": telephone_value,
                    "speed": speed,
                    "reg_number": reg_number,
                    "client_specific_comments": reports
                }
                pdf_files.append(pdf_data)
                save_data_to_table(pdf_files,unextracted_pdf_names)
    return pdf_files


def save_data_to_table(pdf_data,unextracted_pdf_names):
    try:
        conn = connect_to_database()
        if conn:
            cursor = conn.cursor()

            for data in pdf_data:
                sql_insert = """
                    INSERT INTO [dbo].[Usage] (
                    [ClientAccountID],
                    [Name],
                    [ReportID],
                    [Company],
                    [Country],
                    [ServiceType],
                    [Date],
                    [EnteredRefNo],
                    [Telephone],
                    [CompanyRegNum],
                    [Address],
                    [Comments],
                    [ActionTypeId],
                    [IsProcessed],
                    [TransactionSource],
                    [StatusId]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)
                """

                # Execute the INSERT statement with data from pdf_data
                cursor.execute(sql_insert, (
                    'EDCTEST',
                    'EDC',   # ClientAccountID
                    123,   # ReportID
                    data.get("company_name"),
                    data.get("country"),
                    data.get("speed"),
                    data.get("date"),
                    data.get("client_ref"),    # EnteredRefNo
                    data.get("telephone"),
                    data.get("reg_number"),    # CompanyRegNum
                    data.get("address"),       # Address
                    data.get("client_specific_comments"),  # Comments
                    1,
                    0,   
                    'OCR',
                    1  
                ))

            # Commit the transaction
            conn.commit()

            # Update the isextracted field to 1 for each PDF name in the database
            for pdf_name in unextracted_pdf_names:
                cursor.execute("UPDATE pdf_documents SET IsExtracted = 1 WHERE Pdf_name = ?", (pdf_name,))

            # Commit the update
            conn.commit()
            conn.close()
        else:
            print("Database connection failed.")
    except Exception as e:
        print(f"Error saving data to table: {str(e)}")

# Function to find country in address lines
def countryfind(address_lines):
    for i in range(len(address_lines)):
        if "Telephone" in address_lines[i]:
            if i > 0:
                for country in pycountry.countries:
                    if country.name in address_lines[i-1]:
                        return country.name
    if "Telephone" not in address_lines[-1]:
        for country in pycountry.countries:
            if country.name in address_lines[-1]:
                return country.name
    return None

# Function to get the next index
def get_next_index(lst, target_string):
    for i in range(len(lst)):
        if target_string in lst[i]:
             return lst[i+1] if i+1 < len(lst) else None
    return None

def get_unextracted_pdf_names_from_database():
    conn = connect_to_database()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT Pdf_name FROM pdf_documents WHERE Isextracted = 0")
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            print(f"Error fetching unextracted PDF names from database: {str(e)}")
        finally:
            conn.close()
    return []


if __name__ == '__main__':
    process_pdf_files()

