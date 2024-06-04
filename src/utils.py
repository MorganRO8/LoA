# Imports
import itertools
import os
import hashlib
import csv
import importlib
import pandas as pd
import numpy as np
from pdf2image import convert_from_path
import builtins
import datetime
import json
import xml.etree.ElementTree as ET


def print(text):
    """Prints the given text to the console and to a log file, with a timestamp."""
    with open(builtins.a, 'a+') as file:
        builtins.print(f"{datetime.datetime.now()}: {text}", file=file)
    builtins.print(text)


def truncate_filename(directory, filename, max_path_length=255):
    """
    Truncate a filename to ensure the entire path stays within a given maximum length 
    and append a short hash to ensure uniqueness.
    """
    # Calculate the remaining allowable length for the filename
    remaining_length = max_path_length - len(directory) - len("arXiv_") - len(".pdf") - 1  # account for separators and extensions

    if len(filename) <= remaining_length:
        return filename

    # Create a short hash of the filename
    short_hash = hashlib.sha1(filename.encode()).hexdigest()[:5]
    truncated_name = filename[:remaining_length - len(short_hash) - 1]
    return f"{truncated_name}_{short_hash}"


def select_search_info_file():
    search_info_dir = os.path.join(os.getcwd(), 'search_info')
    search_info_files = [file for file in os.listdir(search_info_dir) if file.endswith('.txt')]

    print("Available search info files:")
    for i, file in enumerate(search_info_files):
        print(f"{i + 1}. {file}")

    while True:
        choice = input("Enter the number of the search info file you want to use: ")
        if choice.isdigit() and 1 <= int(choice) <= len(search_info_files):
            return os.path.join(search_info_dir, search_info_files[int(choice) - 1])
        else:
            print("Invalid choice. Please try again.")

def select_data_model_file():
    data_models_dir = os.path.join(os.getcwd(), 'dataModels')
    data_model_files = [file for file in os.listdir(data_models_dir) if file.endswith('.py')]

    print("Available data model files:")
    for i, file in enumerate(data_model_files):
        print(f"{i + 1}. {file}")

    while True:
        choice = input("Enter the number of the data model file you want to use: ")
        if choice.isdigit() and 1 <= int(choice) <= len(data_model_files):
            return data_model_files[int(choice) - 1]
        else:
            print("Invalid choice. Please try again.")


def get_out_id(def_search_terms_input, maybe_search_terms_input):

    if def_search_terms_input[0].lower() == "none" or def_search_terms_input[0] == '':
        print("No definite search terms selected.")
        def_search_terms = None
        
    elif type(def_search_terms_input) == str:
        print("String input detected for def search terms")
        def_search_terms = [term.strip() for term in def_search_terms_input.split(",")]
        def_search_terms.sort()
        
    elif type(def_search_terms_input) == list:
        print("List input detected for def search terms")
        print(f"def_search_terms_input = {def_search_terms_input}")
        def_search_terms = def_search_terms_input
        def_search_terms.sort()
        print(f"def_search_terms = {def_search_terms}")
        
    else:
        print(f"def search terms should be str or list, but it is instead {type(def_search_terms_input)}")


    if maybe_search_terms_input[0].lower() == "none" or def_search_terms_input[0] == '':
        print("No maybe search terms selected, only using definite search terms.")
        maybe_search_terms = None
        
    elif type(maybe_search_terms_input) == str:
        print("String input detected for maybe search terms")
        maybe_search_terms = [term.strip() for term in maybe_search_terms_input.split(",")]
        maybe_search_terms.sort()
        
    elif type(maybe_search_terms_input) == list:
        print("List input detected for maybe search terms")
        print(f"maybe_search_terms_input = {maybe_search_terms_input}")
        maybe_search_terms = maybe_search_terms_input
        maybe_search_terms.sort()
        print(f"maybe_search_terms = {maybe_search_terms}")
        
    else:
        print(f"maybe search terms should be str or list, but it is instead {type(maybe_search_terms_input)}")

    # Check that at least one of def_search_terms or maybe_search_terms is not None
    if def_search_terms is None and maybe_search_terms is None:
        print("Error: Both definite and maybe search terms cannot be None.")
        return

    if maybe_search_terms is not None:
        if def_search_terms is not None:
            output_directory_id = f"{'_'.join(['def'] + def_search_terms + ['maybe'] + maybe_search_terms).replace(' ', '')}"

            # define queries as all the combinations of 'maybe contains' search terms
            combinations = list(itertools.chain.from_iterable(
                itertools.combinations(maybe_search_terms, r) for r in range(0, len(maybe_search_terms) + 1)))
            queries = [def_search_terms + list(comb) for comb in combinations]
        else:
            output_directory_id = f"{'_'.join(['maybe'] + maybe_search_terms).replace(' ', '')}"

            # define queries as all the combinations of 'maybe contains' search terms
            combinations = list(itertools.chain.from_iterable(
                itertools.combinations(maybe_search_terms, r) for r in range(1, len(maybe_search_terms) + 1)))
            queries = [list(comb) for comb in combinations]
            queries = [q for q in queries if len(q) > 1]

        print(" Ok! your adjusted searches are: " + str(queries))
        print("That's " + str(len(queries)) + " total combinations")
        if len(queries) > 100:
            print("This could take a while...")

        query_chunks = queries

    else:
        output_directory_id = f"{'_'.join(['def'] + def_search_terms).replace(' ', '')}"

        query_chunks = [def_search_terms]

    return output_directory_id, query_chunks
    
    
def list_files_in_directory(directory):
    """List all files in the given directory."""
    return [file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
    
    
def has_multiple_columns(pdf_path):
        # Convert the first page of the PDF to an image
        images = convert_from_path(pdf_path, dpi=200, first_page=1, last_page=1)
        image = images[0]

        # Convert the image to grayscale and binarize it
        image = image.convert('L')
        image = image.point(lambda x: 0 if x < 128 else 255, '1')

        # Convert the image to a NumPy array for easier processing
        image_array = np.array(image)

        # Get the dimensions of the image
        height, width = image_array.shape

        # Scan the middle of the image vertically
        for x in range(width // 2 - 5, width // 2 + 5):
            white_pixels = 0
            for y in range(height):
                if image_array[y, x] == 255:
                    white_pixels += 1
                    if white_pixels > 50:
                        return True
                else:
                    white_pixels = 0

        # If no tall line of white pixels was found, return False
        return False
        
        
def xml_to_string(xml_string):
    formatted_output = ""
    root = ET.fromstring(xml_string)

    def process_element(element, level=0):
        nonlocal formatted_output
        tag = element.tag.split('}')[-1]  # Remove namespace prefix
        text = element.text.strip() if element.text else ""

        if tag == "article-title":
            formatted_output += text + "\n" + "=" * len(text) + "\n\n"
        elif tag in ["p", "sec"]:
            formatted_output += text + "\n\n"
        elif tag == "title":
            formatted_output += "#" * (level + 1) + " " + text + "\n\n"
        elif tag == "abstract":
            formatted_output += "Abstract: " + text + "\n\n"
        elif tag in ["fig", "table-wrap"]:
            formatted_output += f"{tag.capitalize()}: {text}\n\n"
        elif tag == "caption":
            formatted_output += f"Caption: {text}\n\n"
        elif tag == "table":
            formatted_output += f"Table: {text}\n\n"
        elif tag not in ["ref", "element-citation", "person-group", "name", "surname", "given-names", "article-title", "source", "year", "volume", "fpage", "lpage", "pub-id"]:
            formatted_output += f"{text}"

        for child in element:
            process_element(child, level + 1)

    process_element(root)
    return formatted_output
        
        
def elements_to_string(elements_list):
    formatted_output = ""

    for element in elements_list:
        element_type = element.get("type")
        text = element.get("text", "")

        if element_type == "Title":
            formatted_output += text + "\n" + "=" * len(text) + "\n\n"
        elif element_type in ["Text", "NarrativeText", "UncategorizedText"]:
            formatted_output += text + "\n\n"
        elif element_type == "BulletedText":
            formatted_output += "* " + text + "\n"
        elif element_type == "Abstract":
            formatted_output += "Abstract: " + text + "\n\n"
        elif element_type in ["Form", "Field-Name", "Value", "Link", "CompositeElement"]:
            formatted_output += text + "\n"
        elif element_type in ["Image", "Picture"]:
            formatted_output += "[Image: " + text + "]\n"
        elif element_type in ["FigureCaption", "Caption", "Footnote"]:
            formatted_output += "(" + text + ")\n"
        elif element_type == "Figure":
            formatted_output += "Figure: " + text + "\n"
        elif element_type in ["List", "List-item", "ListItem"]:
            formatted_output += "\n- " + text + "\n"
        elif element_type == "Checked":
            formatted_output += "[x] " + text + "\n"
        elif element_type == "Unchecked":
            formatted_output += "[ ] " + text + "\n"
        elif element_type in ["Address", "EmailAddress"]:
            formatted_output += text + "\n"
        elif element_type == "PageBreak":
            formatted_output += "\n--- Page Break ---\n\n"
        elif element_type == "Formula":
            formatted_output += "Formula: " + text + "\n"
        elif element_type == "Table":
            formatted_output += "Table: " + text + "\n"
        elif element_type in ["Header", "Headline", "Subheadline", "Page-header", "Section-header"]:
            formatted_output += text.upper() + "\n"
        elif element_type in ["Footer", "Page-footer"]:
            formatted_output += "-- " + text + " --\n"
        else:
            formatted_output += f"Other: {text}\n\n"

    return formatted_output
    

def process_output(model_output, pydantic_model):
    try:
        # Parse the model output as a JSON object
        output_data = json.loads(model_output)

        # Validate and parse the output data using the Pydantic model
        validated_data = pydantic_model(**output_data)

        return validated_data

    except (json.JSONDecodeError, ValidationError) as e:
        print(f"Error processing model output: {e}")
        return None
    

def select_schema_file():
    schema_dir = os.path.join(os.getcwd(), 'dataModels')
    schema_files = [file for file in os.listdir(schema_dir) if file.endswith('.pkl')]

    print("Available schema files:")
    for i, file in enumerate(schema_files):
        print(f"{i + 1}. {file}")

    while True:
        choice = input("Enter the number of the schema file you want to use: ")
        if choice.isdigit() and 1 <= int(choice) <= len(schema_files):
            return os.path.join(schema_dir, schema_files[int(choice) - 1])
        else:
            print("Invalid choice. Please try again.")

def load_schema_file(schema_file):
    with open(schema_file, 'r') as f:
        lines = f.readlines()

    key_columns = []
    if lines[0].startswith("Key Columns:"):
        key_columns = [int(column.strip()) for column in lines[0].split(':')[1].strip().split(',') if column.strip()]
        lines = lines[2:]  # Skip the key columns line and the empty line

    schema_data = {}
    for i in range(0, len(lines), 4):
        column_number = int(lines[i].split('-')[0].strip())
        column_type = lines[i].split(':')[1].strip().strip("'")
        column_name = lines[i + 1].split(':')[1].strip().strip("'")
        column_description = lines[i + 2].split(':')[1].strip().strip("'")

        schema_data[column_number] = {
            'type': column_type,
            'name': column_name,
            'description': column_description
        }
        
        if column_type == 'str':
            min_length = lines[i + 3].split(':')[1].strip().strip("'")
            max_length = lines[i + 4].split(':')[1].strip().strip("'")
            whitelist_substrings = lines[i + 5].split(':')[1].strip().strip("'").split(',')
            blacklist_substrings = lines[i + 6].split(':')[1].strip().strip("'").split(',')
            schema_data[column_number]['min_length'] = int(min_length) if min_length else None
            schema_data[column_number]['max_length'] = int(max_length) if max_length else None
            schema_data[column_number]['whitelist_substrings'] = [substring.strip() for substring in whitelist_substrings if substring.strip()]
            schema_data[column_number]['blacklist_substrings'] = [substring.strip() for substring in blacklist_substrings if substring.strip()]
        elif column_type == 'int':
            min_value = lines[i + 3].split(':')[1].strip().strip("'")
            max_value = lines[i + 4].split(':')[1].strip().strip("'")
            schema_data[column_number]['min_value'] = int(min_value) if min_value else None
            schema_data[column_number]['max_value'] = int(max_value) if max_value else None

    print(f"Schema data: {schema_data}")
    return schema_data, key_columns

def generate_prompt(schema_data, user_instructions, key_columns=[]):
    num_columns = len(schema_data)
    schema_info = ""

    for column_number, column_data in schema_data.items():
        schema_info += f"Column {column_number}:\n"
        schema_info += f"Name: {column_data['name']}\n"
        schema_info += f"Description: {column_data['description']}\n"
        schema_info += f"Type of data allowed: {column_data['type']}\n\n"
        
        
    if key_columns != []:
        key_column_names = []
        
        for key_column in key_columns:
            key_column_names.append(schema_data[int(key_column)]['name'])
    
        key_column_info = f"The column(s) {key_column_names} will be used as a key to check for duplicates within each paper. Please ensure that the values in this column are unique for each row extracted from the same paper."
    else:
        key_column_info = ""

    prompt = f"""
    System: 
    
    Instruction: Please extract all information from the provided paper that fits into the CSV schema defined below. There are {num_columns} specific fields of information we are trying to extract. The information will ultimately be put inside an excel spreadsheet with the following layout:
    {schema_info}
    {key_column_info}
    

    General instructions for every extraction task:
    Remember this information is going to be put in a spreadsheet, so grouping of information matters. Please extract any occurrences of the specified information from the text and provide them back as comma separated values, where newline characters represent moving to the next row of the spreadsheet. If you can find information for some of a row, but not all of it, still provide it in your response, but fill in the missing information with 'null' only, which will signal the post-processing that there is no value available for that entry. Make sure to adhere to this structure exactly for your entire response, because natural language wull cause errors as everything in your response will be appended directly to a CSV file. If you don't find an instance of the information available, simply respond with triple bar characters: ||| and this will trigger the code to stop retrying the information extraction. Remember we have {num_columns} columns in the target CSV, so if you find information that fits, your response should be a number of entries that is a multiple of {num_columns}, all of which are separated by a comma, as in the source of a CSV file. Do not explain anything, talk to me in natural language, or try to work as a chat agent. Instead, reply only with information in the specified format. Note that the document you will be reading has been extracted using optical character recognition, so there will be some artifacts of that you will need to work around. Ignore the references, I know they are obtuse, but there is no good way to exclude them. Do not include a title or the headers in your response, or any foratting that does not strictly follow the provided format. As well, make sure you understand that each comma in your response corresponds to a new column, and each newline character corresponds to a new row. Overall, be very careful and deliberate in your response, so you don't cause errors! Thank you!
    
    Something to keep in mind as well, is that the text you will be given is from a scraping process, that sometimes may be tough to interpret. To keep the results accurate, you should only pull information thar matches the format, but also only information that is well supported by the text. Things like titles of referenced papers, single sentences surrounded by random characters picked up by the OCR for pdfs, and formatting artifacts from sources like pubmed, tell you that you should not be including that information, even if it seems relevant. Basically, please consider context (or lack thereof) and try to use context to gauge whether or not you should include a piece of information.
    
    
    Specific instructions for this extraction task:
    {user_instructions}
    """

    return prompt

def parse_llm_response(response, num_columns):
    lines = response.strip().split('\n')
    parsed_data = []
    current_span = []
    max_span = []

    for line in lines:
        columns = line.split(',')
        if len(columns) == num_columns:
            current_span.append(columns)
        else:
            if len(current_span) > len(max_span):
                max_span = current_span
            current_span = []

    if len(current_span) > len(max_span):
        max_span = current_span

    if len(max_span) == 1 and len(parsed_data) == 0:
        parsed_data = [line.split(',') for line in lines if len(line.split(',')) == num_columns]
    else:
        parsed_data = max_span

    # Remove duplicate entries
    unique_data = []
    for row in parsed_data:
        if row not in unique_data:
            unique_data.append(row)

    return unique_data

def process_value(value, column_data):
    column_type = column_data['type']
    if column_type == 'int':
        processed_value = int(''.join(filter(str.isdigit, value)))
        min_value = column_data.get('min_value')
        max_value = column_data.get('max_value')
        if min_value is not None and processed_value < min_value:
            raise ValueError(f"Value {processed_value} is less than the minimum allowed value {min_value}")
        if max_value is not None and processed_value > max_value:
            raise ValueError(f"Value {processed_value} is greater than the maximum allowed value {max_value}")
        return processed_value
    elif column_type == 'str':
        processed_value = value.strip()
        min_length = column_data.get('min_length')
        max_length = column_data.get('max_length')
        whitelist_substrings = column_data.get('whitelist_substrings')
        blacklist_substrings = column_data.get('blacklist_substrings')
        if min_length is not None and len(processed_value) < min_length:
            raise ValueError(f"String '{processed_value}' is shorter than the minimum allowed length {min_length}")
        if max_length is not None and len(processed_value) > max_length:
            raise ValueError(f"String '{processed_value}' is longer than the maximum allowed length {max_length}")
        if whitelist_substrings:
            for substring in whitelist_substrings:
                if substring not in processed_value:
                    raise ValueError(f"String '{processed_value}' does not contain the required substring '{substring}'")
        if blacklist_substrings:
            for substring in blacklist_substrings:
                if substring in processed_value:
                    raise ValueError(f"String '{processed_value}' contains the blacklisted substring '{substring}'")
        return processed_value
    elif column_type == 'float':
        return float(''.join(filter(lambda x: x.isdigit() or x == '.', value)))
    elif column_type == 'complex':
        return complex(''.join(filter(lambda x: x.isdigit() or x in ['+', '-', 'j', '.'], value)))
    elif column_type == 'range':
        range_parts = value.replace(' ', '').split('-')
        if len(range_parts) == 2:
            return f"{float(range_parts[0])}-{float(range_parts[1])}"
        else:
            return value
    elif column_type == 'boolean':
        lower_value = value.lower().strip()
        if 'true' in lower_value:
            return True
        elif 'false' in lower_value:
            return False
        else:
            return value
    else:
        return value

def validate_result(parsed_result, schema_data):
    num_columns = len(schema_data)
    validated_result = []

    for row in parsed_result:
        if len(row) != num_columns:
            raise ValueError(f"Invalid number of columns in row: {row}")

        validated_row = []
        for i, value in enumerate(row):
            if value != 'null':
                column_type = schema_data[i + 1]['type']
                try:
                    processed_value = process_value(value, column_type)
                    validated_row.append(processed_value)
                except Exception as e:
                    raise ValueError(f"Error processing value '{value}' for column {i + 1} (type: {column_type}): {type(e).__name__} - {str(e)}")
            else:
                validated_row.append('null')

        validated_result.append(validated_row)

    # Check if headers are present in the validated result
    headers_present = True
    if validated_result and len(validated_result[0]) == num_columns:
        for i in range(num_columns):
            if schema_data[i + 1]['name'] not in validated_result[0][i]:
                headers_present = False
                break
    else:
        headers_present = False

    if headers_present:
        validated_result.pop(0)

    return validated_result

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def write_to_csv(data, headers, filename="extracted_data.csv"):
    file_exists = os.path.isfile(filename)
    with open(filename, mode='a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow(headers)
        writer.writerows(data)