from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from langchain import PromptTemplate
import json
import os
import pandas as pd
# from PIL import Image as PILImage
# from openpyxl import load_workbook
# from io import BytesIO
# import easyocr
# import numpy as np
# import re
from fuzzywuzzy import fuzz
import re
 
# reader = easyocr.Reader(['en'])
 
load_dotenv()
 
api_key = os.getenv("API_KEY")
azure_endpoint = os.getenv("AZURE_ENDPOINT")
openai_api_version = os.getenv("OPENAI_API_VERSION")
deployment_name = os.getenv("DEPLOYMENT_NAME")
 
llm = AzureChatOpenAI(
    openai_api_version=openai_api_version,
    azure_endpoint=azure_endpoint,
    openai_api_key=api_key,
    openai_api_type="azure",
    deployment_name=deployment_name,
    model="gpt-4o",
    temperature=0.0,
)
 
function_descriptions = [
    {
        "name": "Scan_Quotation",
        "description": "Scans a Quotation from vendor and returns relevant information",
        "parameters": {
            "type": "object",
            "properties": {
                "Vendor Name": {
                    "type": "string",
                    "description": "Name of the vendor"
                },
                "Quote ID": {
                    "type": "string",
                    "description": "Id of Quotation"
                },
                "Date": {
                    "type": "string",
                    "description": "Date of Quotation and return in 'dd-mm-yyyy' format"
                },
                "Currency": {
                    "type": "string",
                    "description": "Currency of Items Amount"
                },
                "Product Details": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "Material": {
                                "type": "string",
                                "description": "Name of Material from Quotation"
                            },
                            "Description": {
                                "type": "string",
                                "description": "Description of Item"
                            },
                            "Total Amount": {
                                "type": "string",
                                "description": "Total Amount of Item"
                            },
                            "Quantity": {
                                "type": "string",
                                "description": "Quantity of Item." # Do not give hours like 6 hrs in quantity.
                            },
                            "Unit Cost": {
                                "type": "string",
                                "description": "Cost of each Item."
                            },
                            "Country": {
                                "type": "string",
                                "description": "Country from which the vendor belongs"
                            },
                            "City": {
                                "type": "string",
                                "description": "Country from which the vendor belongs"
                            },
               
                        },
                    },
                    "description": "Products details from the Quotation",
                },
                },
            "required": ["Vendor Name", "Quote ID", "Date"]
        }
    }
]
 
function_descriptions_format_b = [
    {
        "name": "Scan_Quotation_Format_B",
        "description": "Scans a Quotation from vendor (Format B) and returns relevant information including installation, disposal, and freight details.",
        "parameters": {
            "type": "object",
            "properties": {
                "Vendor Name": {
                    "type": "string",
                    "description": "Name of the vendor or supplier"
                },
                "Quote ID": {
                    "type": "string",
                    "description": "ID of the Quotation"
                },
                "Date": {
                    "type": "string",
                    "description": "Date of Quotation, formatted as 'dd-mm-yyyy'"
                },
                "Currency": {
                    "type": "string",
                    "description": "Currency used in the Quotation"
                },
                "Product Details": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "Material": {
                                "type": "string",
                                "description": "Specific item or service provided for installation"
                            },
                            "Description": {
                                "type": "string",
                                "description": "Detailed description of the item or service"
                            },
                            "Total Amount": {
                                "type": "string",
                                "description": "Total amount for the item/service"
                            },
                            "Quantity": {
                                "type": "string",
                                "description": "Quantity of the item/service. Do not give hours like 6 hrs in quantity."
                            },
                            "Hours": {
                                "type": "string",
                                "description": "Number of hours (for services)"
                            },
                            "Unit Cost": {
                                "type": "string",
                                "description": "Cost per unit of the item/service. If there is 'Selling Price', then assume 'Selling Price' as unit cost."
                            },
                            "Country": {
                                "type": "string",
                                "description": "Country from or to which the vendor is supplying. Get data for each and every Country if there is more than one countries of data is available."
                            },
                            "City": {
                                "type": "string",
                                "description": "City of the vendor or supply location. Get data for each and every City of a Country if there is more than one city of data is available for a particular country."
                            },
                           
                        },
                    },
                    "description": "List of products and services mentioned in the Quotation."
                }
            },
            "required": ["Vendor Name", "Quote ID", "Date"]
        }
    }
]
 
template = """
Scan the following vendor quotation and return all relevant details. If the data is missing, return an empty string ('') for the missing fields. You are required to extract any type of data, including but not limited to:
 
- Installation details
- Disposal details
- Freight details
- Maintenance details
- Any other services or materials listed
 
For each type of data, extract the following information:
 
- Type of Service/Material: Name or description of the service or material
- Description: Detailed description of the service or material provided
- Total Cost: The total amount or cost associated with the service or material
- Quantity: Quantity provided (if applicable)
- Unit Cost: Cost per unit of the service or material (if applicable)
- Country: Country associated with the service or material (if available)
- City: City associated with the service or material (if available)
 
Do not extract the details of Sub Total that ends in last of the documents.
 
### Now, scan the following Quotation and return the details in the same format:
 
Quotation: {quote}
"""
 
def insert_data_to_sql(data, conn):
    cursor = conn.cursor()
 
    sql_insert_sp = """EXEC InsertQuotationDetails @QuotationDetails = ?"""
   
    chunk_size = 1000
    for start in range(0, len(data), chunk_size):
        chunk = data[start:start + chunk_size]
       
        quotation_details_list = [tuple(row) for row in chunk.itertuples(index=False)]
       
        cursor.execute(sql_insert_sp, [quotation_details_list])
       
        conn.commit()
       
    cursor.close()
 
def is_format_b(sheet_name):
    format_b_sheet_names = ["OPTY3478", "Sites_Quote", "SOW", "Partner's Terms & Conditions", "BOM", "Change Log", "Pricing Assumptions"]
    threshold = 80  
    match_count = 0
 
    if sheet_name:
        for common_name in format_b_sheet_names:
            score = fuzz.partial_ratio(sheet_name.lower(), common_name.lower())
            if score > threshold:
                match_count += 1
                if match_count >= 2:  
                    return True
    return False
 
# def excel_column_to_number(column):
#     """Convert Excel column letters to a number (A=1, B=2, etc.)."""
#     num = 0
#     for char in column:
#         if 'A' <= char <= 'Z':
#             num = num * 26 + (ord(char) - ord('A')) + 1
#     return num
 
# def convert_excel_cell(cell):
#     """Convert Excel-style cell reference (e.g., A1) to row, col indices."""
#     match = re.match(r"([A-Z]+)([0-9]+)", cell, re.I)
#     if match:
#         col_str, row_str = match.groups()
#         col = excel_column_to_number(col_str)  
#         row = int(row_str)  
#         return row, col
#     else:
#         raise ValueError(f"Invalid cell address: {cell}")
 
# def extract_text_from_image_easyocr(image_bytes):
#     """Extract text from an image using EasyOCR"""
#     try:
#         img = PILImage.open(BytesIO(image_bytes))
#         img = img.convert('RGB')  
#         img_np = np.array(img)  
 
#         result = reader.readtext(img_np)
 
#         extracted_text = " ".join([item[1] for item in result])
#         return extracted_text
 
#     except Exception as e:
#         print(f"Error with EasyOCR: {e}")
#         return ""
 
# def extract_images_from_excel(file_path):
#     """Extract images from Excel, perform OCR, and replace with text"""
#     wb = load_workbook(file_path)
#     ws = wb.active
 
#     image_text_map = {}
 
#     for image in ws._images:
#         cell_position = image.anchor._from
#         cell_address = ws.cell(row=cell_position.row + 1, column=cell_position.col + 1).coordinate
#         img_bytes = image._data()
#         extracted_text = extract_text_from_image_easyocr(img_bytes)
#         image_text_map[cell_address] = extracted_text.strip()
 
#     for cell, text in image_text_map.items():
#         ws[cell] = text
 
#     return image_text_map
 
def is_float(value):
    """Helper function to check if a string can be converted to a float."""
    try:
        float(value)
        return True
    except ValueError:
        return False
   
def convert_to_usd(amount, currency):
    # Example conversion rates; in a real application, fetch these from an API or a reliable source
    conversion_rates = {
    "AED": 0.27,   # United Arab Emirates Dirham
    "AFN": 0.011,  # Afghan Afghani
    "ALL": 0.009,  # Albanian Lek
    "AMD": 0.0026, # Armenian Dram
    "ANG": 0.56,   # Netherlands Antillean Guilder
    "AOA": 0.0018, # Angolan Kwanza
    "ARS": 0.0027, # Argentine Peso
    "AUD": 0.75,   # Australian Dollar
    "AWG": 0.56,   # Aruban Florin
    "AZN": 0.59,   # Azerbaijani Manat
    "BAM": 0.56,   # Bosnia and Herzegovina Convertible Mark
    "BBD": 0.50,   # Barbadian Dollar
    "BDT": 0.0092, # Bangladeshi Taka
    "BGN": 0.56,   # Bulgarian Lev
    "BHD": 2.65,   # Bahraini Dinar
    "BIF": 0.00054,# Burundian Franc
    "BMD": 1.00,   # Bermudian Dollar
    "BND": 0.74,   # Brunei Dollar
    "BOB": 0.14,   # Bolivian Boliviano
    "BRL": 0.20,   # Brazilian Real
    "BSD": 1.00,   # Bahamian Dollar
    "BTN": 0.012,  # Bhutanese Ngultrum
    "BWP": 0.090,  # Botswana Pula
    "BYN": 0.40,   # Belarusian Ruble
    "BZD": 0.50,   # Belize Dollar
    "CAD": 0.80,   # Canadian Dollar
    "CDF": 0.00051,# Congolese Franc
    "CHF": 1.1,    # Swiss Franc
    "CLP": 0.0012, # Chilean Peso
    "CNY": 0.14,   # Chinese Yuan
    "COP": 0.00025,# Colombian Peso
    "CRC": 0.0018, # Costa Rican Colón
    "CUP": 0.042,  # Cuban Peso
    "CZK": 0.046,  # Czech Koruna
    "DKK": 0.15,   # Danish Krone
    "DOP": 0.018,  # Dominican Peso
    "DZD": 0.0073, # Algerian Dinar
    "EGP": 0.032,  # Egyptian Pound
    "ERN": 0.067,  # Eritrean Nakfa
    "ETB": 0.020,  # Ethiopian Birr
    "EUR": 1.1,    # Euro
    "FJD": 0.47,   # Fijian Dollar
    "FKP": 1.3,    # Falkland Islands Pound
    "GBP": 1.3,    # British Pound
    "GEL": 0.37,   # Georgian Lari
    "GHS": 0.085,  # Ghanaian Cedi
    "GIP": 1.3,    # Gibraltar Pound
    "GMD": 0.020,  # Gambian Dalasi
    "GNF": 0.00011,# Guinean Franc
    "GTQ": 0.13,   # Guatemalan Quetzal
    "GYD": 0.0048, # Guyanaese Dollar
    "HKD": 0.13,   # Hong Kong Dollar
    "HNL": 0.040,  # Honduran Lempira
    "HRK": 0.14,   # Croatian Kuna
    "HTG": 0.012,  # Haitian Gourde
    "HUF": 0.0030, # Hungarian Forint
    "IDR": 0.000065,# Indonesian Rupiah
    "ILS": 0.29,   # Israeli New Shekel
    "INR": 0.012,  # Indian Rupee
    "IQD": 0.00068,# Iraqi Dinar
    "IRR": 0.000024,# Iranian Rial
    "ISK": 0.0075, # Icelandic Króna
    "JMD": 0.0065, # Jamaican Dollar
    "JOD": 1.41,   # Jordanian Dinar
    "JPY": 0.007,  # Japanese Yen
    "KES": 0.0070, # Kenyan Shilling
    "KGS": 0.012,  # Kyrgyzstani Som
    "KHR": 0.00025,# Cambodian Riel
    "KPW": 0.0011, # North Korean Won
    "KRW": 0.00074,# South Korean Won
    "KWD": 3.24,   # Kuwaiti Dinar
    "KZT": 0.0021, # Kazakhstani Tenge
    "LAK": 0.000084,# Lao Kip
    "LBP": 0.000066,# Lebanese Pound
    "LKR": 0.0027, # Sri Lankan Rupee
    "LRD": 0.0048, # Liberian Dollar
    "LYD": 0.21,   # Libyan Dinar
    "MAD": 0.096,  # Moroccan Dirham
    "MDL": 0.052,  # Moldovan Leu
    "MKD": 0.018,  # Macedonian Denar
    "MMK": 0.00051,# Myanmar Kyat
    "MNT": 0.00026,# Mongolian Tögrög
    "MOP": 0.12,   # Macanese Pataca
    "MRU": 0.026,  # Mauritanian Ouguiya
    "MUR": 0.024,  # Mauritian Rupee
    "MXN": 0.055,  # Mexican Peso
    "MYR": 0.22,   # Malaysian Ringgit
    "MZN": 0.015,  # Mozambican Metical
    "NAD": 0.067,  # Namibian Dollar
    "NGN": 0.0012, # Nigerian Naira
    "NIO": 0.036,  # Nicaraguan Córdoba
    "NOK": 0.097,  # Norwegian Krone
    "NPR": 0.0075, # Nepalese Rupee
    "NZD": 0.72,   # New Zealand Dollar
    "OMR": 2.60,   # Omani Rial
    "PAB": 1.00,   # Panamanian Balboa
    "PEN": 0.27,   # Peruvian Sol
    "PGK": 0.28,   # Papua New Guinean Kina
    "PHP": 0.018,  # Philippine Peso
    "PKR": 0.0036, # Pakistani Rupee
    "PLN": 0.24,   # Polish Zloty
    "PYG": 0.00014,# Paraguayan Guarani
    "QAR": 0.27,   # Qatari Rial
    "RON": 0.22,   # Romanian Leu
    "RSD": 0.0089, # Serbian Dinar
    "RUB": 0.011,  # Russian Ruble
    "RWF": 0.00091,# Rwandan Franc
    "SAR": 0.27,   # Saudi Riyal
    "SBD": 0.13,   # Solomon Islands Dollar
    "SCR": 0.071,  # Seychellois Rupee
    "SDG": 0.0022, # Sudanese Pound
    "SEK": 0.093,  # Swedish Krona
    "SGD": 0.74,   # Singapore Dollar
    "SLL": 0.000059,# Sierra Leonean Leone
    "SOS": 0.0018, # Somali Shilling
    "SRD": 0.14,   # Surinamese Dollar
    "THB": 0.028,  # Thai Baht
    "TJS": 0.11,   # Tajikistani Somoni
    "TMT": 0.29,   # Turkmenistani Manat
    "TND": 0.33,   # Tunisian Dinar
    "TOP": 0.42,   # Tongan Paʻanga
    "TRY": 0.036,  # Turkish Lira
    "TTD": 0.15,   # Trinidad and Tobago Dollar
    "TWD": 0.032,  # New Taiwan Dollar
    "TZS": 0.00043,# Tanzanian Shilling
    "UAH": 0.027,  # Ukrainian Hryvnia
    "UGX": 0.00027,# Ugandan Shilling
    "UYU": 0.028,  # Uruguayan Peso
    "UZS": 0.00009,# Uzbekistani Som
    "VES": 0.000032,# Venezuelan Bolívar
    "VND": 0.000042,# Vietnamese Dong
    "XAF": 0.0017, # Central African CFA Franc
    "XAG": 23.20,  # Silver Ounce
    "XAU": 1840.00,# Gold Ounce
    "XCD": 0.37,   # East Caribbean Dollar
    "XOF": 0.0017, # West African CFA Franc
    "XPF": 0.0093, # CFP Franc
    "YER": 0.0040, # Yemeni Rial
    "ZAR": 0.067,  # South African Rand
    "ZMW": 0.048,  # Zambian Kwacha
}
 
   
    if currency in conversion_rates:
        return amount * conversion_rates[currency]
    else:
        return amount
 
def process_excel(file_path, sheet_name, chunk_size=200):
    try:
        # image_text_map = None
        text = ""
        is_format_b_file = False
       
        if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            is_format_b_file = is_format_b(sheet_name)
 
            # if file_path.endswith(".xlsx"):
            #     image_text_map = extract_images_from_excel(file_path)
 
            processed_df = pd.DataFrame()
 
            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
 
                # if image_text_map is not None:
                #     for cell, ocr_text in image_text_map.items():
                #         row, col = convert_excel_cell(cell)
                #         if row <= chunk.shape[0] and col <= chunk.shape[1]:
                #             if chunk.dtypes.iloc[col - 1] != 'object':
                #                 chunk.iloc[:, col - 1] = chunk.iloc[:, col - 1].astype('object')
 
                #             if pd.notna(ocr_text) and ocr_text != '':
                #                 chunk.iat[row - 1, col - 1] = ocr_text
 
                text = chunk.to_string(index=False)
                text = text.replace('NaN', '')
                prompt = PromptTemplate.from_template(template)
                content = prompt.format(quote=text)
 
                if is_format_b_file:
                    response = llm.invoke(
                        input=content,
                        functions=function_descriptions_format_b
                    )
                else:
                    response = llm.invoke(
                        input=content,
                        functions=function_descriptions
                    )
 
                try:
                    function_call = response.additional_kwargs['function_call']
                    json_data = function_call['arguments']
 
                    last_valid_brace = json_data.rfind('},')
                    if last_valid_brace != -1:
                        json_data = json_data[:last_valid_brace + 1] + ']}'  
 
                    data = json.loads(json_data)
 
                except json.JSONDecodeError as json_err:
                    print(f"JSONDecodeError: {json_err.msg} at line {json_err.lineno}, col {json_err.colno}")
                    print(f"Raw JSON response: {function_call['arguments']}")
                    continue  
 
                table_data = []
                for details in data.get('Product Details', []):
                    total_cost = details.get('Total Amount', '')
                    qty = details.get('Quantity', '')
                    unit_cost = details.get('Unit Cost', '')
                    currency = data.get('Currency', '')
 
                    if unit_cost == '' and is_float(total_cost) and is_float(qty) and float(qty) != 0:
                        unit_cost = float(total_cost) / float(qty)
 
                    if currency != 'USD' and is_float(unit_cost):
                        unit_cost_usd = convert_to_usd(float(unit_cost), currency)
                    else:
                        unit_cost_usd = unit_cost
 
                    table_data.append({
                        'Date': data.get('Date', ''),
                        'Item': details.get('Material', ''),
                        'Description': details.get('Description', ''),
                        'Country': details.get('Country', ''),
                        'City': details.get('City', ''),
                        'Supplier': data.get('Vendor Name', ''),
                        'Quote ID': data.get('Quote ID', ''),
                        'Currency': data.get('Currency', ''),
                        'Total Cost': total_cost,
                        'QTY': qty,
                        'Hours': '',
                        'Unit Cost': unit_cost,
                        'Unit Cost (USD)': unit_cost_usd
                    })
 
                processed_df = pd.concat([processed_df, pd.DataFrame(table_data)])
 
            return processed_df
 
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return pd.DataFrame()
 

    