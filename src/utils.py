# Imports
import itertools
import os
import re
import hashlib
import csv
import numpy as np
from pdf2image import convert_from_path
import builtins
import datetime
import random
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
import zipfile
import io
from selenium.webdriver.chrome.service import Service
from selenium.common import TimeoutException
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm
from datetime import datetime
import xml.etree.ElementTree as ET
import time
import json
import logging
from urllib.parse import quote
from bs4 import BeautifulSoup
import requests


CONVERT_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={}&format=json"
EUTILS_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={}"
ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'


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
    remaining_length = max_path_length - len(directory) - len("arXiv_") - len(
        ".pdf") - 1  # account for separators and extensions

    if len(filename) <= remaining_length:
        return filename

    # Create a short hash of the filename
    short_hash = hashlib.sha1(filename.encode()).hexdigest()[:5]
    truncated_name = filename[:remaining_length - len(short_hash) - 1]
    return f"{truncated_name}_{short_hash}"


def select_search_info_file():
    search_info_dir = os.path.join(os.getcwd(), 'search_info')
    search_info_files = [file for file in os.listdir(search_info_dir) if file.endswith('.txt')]
    search_info_files.insert(0, 'All')

    print("Available search info files:")
    for i, file in enumerate(search_info_files):
        print(f"{i + 1}. {file}")

    while True:
        choice = input("Enter the number of the search info file you want to use: ")
        if int(choice) == 1:
            return 'All'
        if choice.isdigit() and 2 <= int(choice) <= len(search_info_files):
            return os.path.join(search_info_dir, search_info_files[int(choice) - 1])
        else:
            print("Invalid choice. Please try again.")


def select_data_model_file():
    data_models_dir = os.path.join(os.getcwd(), 'dataModels')
    data_model_files = [file for file in os.listdir(data_models_dir) if file.endswith('.pkl')]

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

    if maybe_search_terms_input[0].lower() == "none" or maybe_search_terms_input[0] == '':
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


def doi_to_filename(doi: str) -> str:
    # Define replacements for invalid filename characters
    replacements = {
        '/': '_SLASH_',
        ':': '_COLON_'
    }

    # Replace each invalid character with its replacement
    filename = doi
    for char, replacement in replacements.items():
        filename = filename.replace(char, replacement)

    # Optionally replace any other invalid characters (e.g., non-alphanumeric)
    filename = re.sub(r'[^\w\-\.]', '_OTHER_', filename)

    return filename


def filename_to_doi(filename: str) -> str:
    # Define replacements for invalid filename characters
    replacements = {
        '_SLASH_': '/',
        '_COLON_': ':'
    }

    # Replace each replacement with its original character
    doi = filename
    for replacement, char in replacements.items():
        doi = doi.replace(replacement, char)

    # Optionally replace any other placeholder (assuming you have a rule for those)
    doi = re.sub(r'_OTHER_', '', doi)

    return doi


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
        elif tag not in ["ref", "element-citation", "person-group", "name", "surname", "given-names", "article-title",
                         "source", "year", "volume", "fpage", "lpage", "pub-id"]:
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
        lines = lines[1:]  # Skip the key columns line

    schema_data = {}
    current_column = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        parts = line.split('-')
        if len(parts) == 2:
            column_number = int(parts[0].strip())
            info_type, info_value = parts[1].split(':', 1)
            info_type = info_type.strip()
            info_value = info_value.strip().strip("'")

            if info_type == "Type":
                current_column = column_number
                schema_data[current_column] = {'type': info_value}
            elif current_column is not None:
                if info_type == "Name":
                    schema_data[current_column]['name'] = info_value
                elif info_type == "Description":
                    schema_data[current_column]['description'] = info_value
                elif info_type == "Allowed Values":
                    schema_data[current_column]['allowed_values'] = [value.strip() for value in info_value.split(',') if
                                                                     value.strip()]
                elif info_type == "Min Length":
                    schema_data[current_column]['min_length'] = int(info_value) if info_value else None
                elif info_type == "Max Length":
                    schema_data[current_column]['max_length'] = int(info_value) if info_value else None
                elif info_type == "Min Value":
                    schema_data[current_column]['min_value'] = float(info_value) if info_value else None
                elif info_type == "Max Value":
                    schema_data[current_column]['max_value'] = float(info_value) if info_value else None
                elif info_type == "Required Substrings":
                    schema_data[current_column]['required_substrings'] = [substring.strip() for substring in
                                                                          info_value.split(',') if substring.strip()]
                elif info_type == "Blacklisted Substrings":
                    schema_data[current_column]['blacklisted_substrings'] = [substring.strip() for substring in
                                                                             info_value.split(',') if substring.strip()]

    # Fill in missing 'description' keys with empty strings
    for column_data in schema_data.values():
        if 'description' not in column_data:
            column_data['description'] = ''

    return schema_data, key_columns


def generate_prompt(schema_data, user_instructions, key_columns=None):
    if key_columns is None:
        key_columns = []
    num_columns = len(schema_data)
    schema_info = ""
    schema_diagram = ""

    for column_number, column_data in schema_data.items():
        schema_info += f"Column {column_number}: {column_data['name']} ({column_data['type']})\n"
        schema_info += f"Description: {column_data['description']}\n\n"
        schema_diagram += f"{column_data['name']}, "
    schema_diagram = schema_diagram[:-2]

    key_column_names = [schema_data[int(column)]['name'] for column in key_columns]
    key_column_info = f"The column(s) {', '.join(key_column_names)} will be used as a key to check for duplicates within each paper. Ensure that the values in these columns are unique for each row extracted."

    prompt = f"""
Please extract information from the provided research paper that fits into the following CSV schema:

{schema_info}
{key_column_info if key_columns else ''}

Instructions:
- Extract relevant information and provide it as comma-separated values.
- Each line should contain {num_columns} values, corresponding to the {num_columns} columns in the schema.
- If information is missing for a column, use 'null' as a placeholder.
- Do not use anything other than 'null' as a placeholder.
- Enclose all string values in quotes.
- For range values, use the format "min-max".
- Do not include headers, explanations, or any additional formatting.
- If no relevant information is found, respond with '|||'.
- Ignore any information in references that may be included at the end of the paper.

Below I shall provide a few examples to help you understand the desired output format.

Here is an example with just the names of the columns instead of actual values, and just a single entry:
{schema_diagram}

Here are a few examples with randomly generated values where appropriate (be sure to note these values mean nothing, and should be ignored, this is just to show the proper structure):

Example where the paper contains a single piece of information to extract:
{generate_examples(schema_data, 1)}

Example where the paper contains two pieces of information to extract:
{generate_examples(schema_data, 2)}

Example where the paper contains three pieces of information to extract:
{generate_examples(schema_data, 3)}

Hopefully that is enough examples for you to see the desired output format.

User Instructions: {user_instructions}

Paper Contents:
    """

    return prompt


def parse_llm_response(response, num_columns):
    lines = response.strip().split('\n')
    lines = [line for line in lines if 'example_string' not in line]
    parsed_data = []

    reader = csv.reader(lines, quotechar='"', skipinitialspace=True)
    for row in reader:
        if len(row) == num_columns:
            parsed_data.append(row)

    # Remove duplicate entries
    unique_data = []
    for row in parsed_data:
        if row not in unique_data:
            unique_data.append(row)

    return unique_data


def normalize_numeric_value(value):
    # Remove any special characters and spaces
    value = re.sub(r'[^\d.eE+-]', '', value)

    # Check if the value is in scientific notation
    if 'e' in value.lower():
        try:
            # Convert scientific notation to float
            value = float(value)
            # Convert float to string with a maximum of 4 decimal places
            value = f"{value:.4f}"
        except ValueError:
            pass

    return value


def process_value(value, column_data):
    column_type = column_data['type']
    if column_type == 'int':
        processed_value = int(''.join(filter(str.isdigit, value)))
        min_value = column_data.get('min_value')
        max_value = column_data.get('max_value')
        allowed_values = column_data.get('allowed_values')
        if allowed_values and str(processed_value) not in allowed_values:
            raise ValueError(f"Value {processed_value} is not in the list of allowed values: {allowed_values}")
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
        allowed_values = column_data.get('allowed_values')
        if allowed_values and str(processed_value) not in allowed_values:
            raise ValueError(f"Value {processed_value} is not in the list of allowed values: {allowed_values}")
        if min_length is not None and len(processed_value) < min_length:
            raise ValueError(f"String '{processed_value}' is shorter than the minimum allowed length {min_length}")
        if max_length is not None and len(processed_value) > max_length:
            raise ValueError(f"String '{processed_value}' is longer than the maximum allowed length {max_length}")
        if whitelist_substrings:
            for substring in whitelist_substrings:
                if substring not in processed_value:
                    raise ValueError(
                        f"String '{processed_value}' does not contain the required substring '{substring}'")
        if blacklist_substrings:
            for substring in blacklist_substrings:
                if substring in processed_value:
                    raise ValueError(f"String '{processed_value}' contains the blacklisted substring '{substring}'")
        while processed_value[0] == " ":
            del processed_value[0]
        while processed_value[-1] == " ":
            del processed_value[-1]
        return processed_value
    elif column_type == 'float':
        processed_value = float(''.join(filter(lambda x: x.isdigit() or x == '.', value)))
        min_value = column_data.get('min_value')
        max_value = column_data.get('max_value')
        allowed_values = column_data.get('allowed_values')
        if allowed_values and str(processed_value) not in allowed_values:
            raise ValueError(f"Value {processed_value} is not in the list of allowed values: {allowed_values}")
        if min_value is not None and processed_value < min_value:
            raise ValueError(f"Value {processed_value} is less than the minimum allowed value {min_value}")
        if max_value is not None and processed_value > max_value:
            raise ValueError(f"Value {processed_value} is greater than the maximum allowed value {max_value}")
        return processed_value
    elif column_type == 'complex':
        return complex(''.join(filter(lambda x: x.isdigit() or x in ['+', '-', 'j', '.'], value)))
    elif column_type == 'range':
        range_parts = value.replace(' ', '').split('-')
        if len(range_parts) == 2:
            minimum = float(re.sub("[^0-9]", "", range_parts[0]))
            maximum = float(re.sub("[^0-9]", "", range_parts[1]))
            return f"{minimum}-{maximum}"
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


def validate_result(parsed_result, schema_data, examples):
    num_columns = len(schema_data)
    validated_result = []
    example_rows = examples.split('\n')

    # Check if headers are present in the parsed result
    headers_present = False
    if parsed_result and len(parsed_result[0]) == num_columns:
        header_row = parsed_result[0]
        for i in range(num_columns):
            if schema_data[i + 1]['name'] == header_row[i]:
                headers_present = True
                break

    if headers_present:
        print("Headers found in response, removing...")
        parsed_result = parsed_result[1:]  # Remove the header row from parsed_result

    for row in parsed_result:
        if len(row) != num_columns:
            print(f"Skipping row with invalid number of columns: {row}")
            continue

        # Check if the row contains example strings
        if any(example_row == ','.join(row) for example_row in example_rows):
            print(f"Skipping row containing example strings: {row}")
            continue

        validated_row = []
        row_valid = True

        for i, value in enumerate(row):
            if value != 'null':
                column_data = schema_data[i + 1]
                try:
                    processed_value = process_value(value, column_data)
                    validated_row.append(processed_value)
                except Exception as e:
                    print(
                        f"Error processing value '{value}' for column {i + 1} (type: {column_data['type']}): {type(e).__name__} - {str(e)}")
                    row_valid = False
                    break
            else:
                validated_row.append('null')

        if row_valid:
            validated_result.append(validated_row)

    if not validated_result:
        print("No valid rows found in the result.")

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


def generate_examples(schema_data, num_examples=3):
    examples = []
    for _ in range(num_examples):
        example_row = []
        for column_number, column_data in schema_data.items():
            column_type = column_data['type']
            allowed_values = column_data.get('allowed_values', [])

            if allowed_values:
                example_value = random.choice(allowed_values)
            elif column_type == 'str':
                example_value = f'"example_string_{column_number}"'
            elif column_type == 'int':
                min_value = column_data.get('min_value', column_number)
                max_value = column_data.get('max_value', column_number + 10)
                example_value = random.randint(min_value, max_value)
            elif column_type == 'float':
                min_value = column_data.get('min_value', float(column_number))
                max_value = column_data.get('max_value', float(column_number) + 1.0)
                example_value = round(random.uniform(min_value, max_value), 2)
            elif column_type == 'range':
                min_value = column_data.get('min_value', column_number)
                max_value = column_data.get('max_value', column_number + 10)

                random.seed(datetime.datetime.now().timestamp())
                rand1 = random.randint(min_value, max_value)

                random.seed(datetime.datetime.now().timestamp())
                rand2 = random.randint(min_value, max_value)

                if rand1 > rand2:
                    temp = rand1
                    rand1 = rand2
                    rand2 = temp

                example_value = f"{rand1}-{rand2}"
            else:
                example_value = f"(example_{column_type}_{column_number})"

            example_row.append(str(example_value))
        examples.append(','.join(example_row))
    return '\n'.join(examples)


def estimate_tokens(text):
    # Split the text into words
    words = re.findall(r'\w+', text)

    # Count the number of words
    word_count = len(words)

    # Estimate the number of tokens
    # A common rule of thumb is that 1 token ≈ 0.75 words
    estimated_tokens = int(word_count / 0.75)

    return estimated_tokens


def truncate_text(text, max_tokens=32000, buffer=3500):
    estimated_total_tokens = estimate_tokens(text)

    if estimated_total_tokens <= max_tokens:
        return text

    # Calculate the proportion of text to keep
    keep_ratio = (max_tokens - buffer) / estimated_total_tokens

    # Split the text into words
    words = text.split()

    # Calculate how many words to keep
    words_to_keep = int(len(words) * keep_ratio)

    # Join the words back together
    truncated_text = ' '.join(words[:words_to_keep])

    return truncated_text


def get_processed_pmids(csv_file):
    processed_pmids = set()
    no_fulltext_pmids = set()

    if os.path.exists(csv_file):
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if row:  # Check if row is not empty
                    processed_pmids.add(row[-1])  # Last column is the PMID

    no_fulltext_file = os.path.join(os.getcwd(), 'search_info', 'no_fulltext.txt')
    if os.path.exists(no_fulltext_file):
        with open(no_fulltext_file, 'r') as f:
            no_fulltext_pmids = set(f.read().splitlines())

    return processed_pmids, no_fulltext_pmids


# Function to download the latest release of ollama binary
# Function to download the latest release of ollama binary with progress bar
def download_ollama():
    # GitHub API URL for the latest releases of ollama
    url = "https://api.github.com/repos/ollama/ollama/releases/latest"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Find the download URL for the amd64 binary
        download_url = None
        for asset in data['assets']:
            if re.search(r'amd64', asset['name'], re.IGNORECASE):
                download_url = asset['browser_download_url']
                break

        if not download_url:
            print("No amd64 binary found in the latest release.")
            return

        # Download the binary with progress bar
        print(f"Downloading ollama binary from {download_url}")
        binary_response = requests.get(download_url, stream=True)
        binary_response.raise_for_status()

        total_size = int(binary_response.headers.get('content-length', 0))
        block_size = 8192  # 8 KB

        with open('ollama', 'wb') as f, tqdm(
                total=total_size, unit='iB', unit_scale=True, desc='ollama'
        ) as progress_bar:
            for chunk in binary_response.iter_content(chunk_size=block_size):
                if chunk:
                    f.write(chunk)
                    progress_bar.update(len(chunk))

        # Make the binary executable
        os.chmod('ollama', 0o755)

        print("ollama binary downloaded and saved successfully.")

    except requests.RequestException as e:
        print(f"Failed to download the binary: {e}")
    except KeyboardInterrupt:
        print("You interrupted before ollama finished downloading, cleaning up file...")
        if os.path.isfile(os.path.join(os.getcwd(), 'ollama')):
            os.remove(os.path.join(os.getcwd(), 'ollama'))
            print("Ollama file deleted, next time please let the download finish!")
        else:
            print("Ollama file not found... weird...")


def get_chrome_driver():
    chrome_version = get_chrome_version()
    print(f"Chrome Version: {chrome_version}")
    driver_path = get_or_download_chromedriver(chrome_version)

    service = Service(driver_path)
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    try:
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except WebDriverException as e:
        if 'executable may have wrong permissions' in str(e):
            print(f"Changing permissions for {driver_path}")
            os.chmod(driver_path, 0o755)  # Add execute permissions
            driver = webdriver.Chrome(service=service, options=options)
            return driver
        else:
            raise


def get_chrome_version():
    # This is specifically for linux
    return os.popen('google-chrome --version').read().strip().split()[-1]


def get_or_download_chromedriver(version):
    driver_path = os.path.join(os.getcwd(), 'chromedriver-linux64', 'chromedriver')

    # Check if chromedriver already exists
    if os.path.exists(driver_path):
        print("ChromeDriver already exists. Skipping download.")
        return driver_path

    version_base = '.'.join(version.split('.')[:3])

    # Use the JSON API to get the latest version information
    json_url = f"https://googlechromelabs.github.io/chrome-for-testing/latest-versions-per-milestone.json"
    response = requests.get(json_url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch version information. Status code: {response.status_code}")

    version_data = response.json()
    milestone = version_base.split('.')[0]

    if milestone not in version_data['milestones']:
        raise Exception(f"No ChromeDriver version found for Chrome {version_base}")

    driver_version = version_data['milestones'][milestone]['version']
    print(f"Latest compatible ChromeDriver version: {driver_version}")

    download_url = f"https://storage.googleapis.com/chrome-for-testing-public/{driver_version}/linux64/chromedriver-linux64.zip"

    print(f"Downloading ChromeDriver from: {download_url}")
    response = requests.get(download_url)

    if response.status_code != 200:
        raise Exception(f"Failed to download ChromeDriver. Status code: {response.status_code}")

    try:
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    except zipfile.BadZipFile:
        raise Exception("Downloaded file is not a valid zip file. The ChromeDriver download might have failed.")

    zip_file.extract('chromedriver-linux64/chromedriver', path=os.getcwd())
    os.rename(os.path.join(os.getcwd(), 'chromedriver-linux64', 'chromedriver'), driver_path)

    os.chmod(driver_path, 0o755)
    print(f"ChromeDriver downloaded and saved to: {driver_path}")
    return driver_path


def scrape_scienceopen(search_terms, retmax):
    LOGGER = logging.getLogger()
    LOGGER.setLevel(logging.WARNING)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-crash-reporter")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-in-process-stack-traces")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--output=/dev/null")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    try:
        driver = get_chrome_driver()
        driver.set_page_load_timeout(60)  # Set timeout to 60 seconds
        wait = WebDriverWait(driver, 10)

        url = (
            f"https://www.scienceopen.com/search#('v'~4_'id'~''_'queryType'~1_'context'~null_'kind'~77_'order'~0_"
            f"'orderLowestFirst'~false_'query'~'{' '.join(search_terms)}'_'filters'~!("
            f"'kind'~84_'openAccess'~true)*_'hideOthers'~false)")

        print("Attempting to load ScienceOpen URL...")
        driver.get(url)
        print("ScienceOpen URL loaded successfully")

        scraped_links_dir = os.path.join(os.getcwd(), 'search_info', 'SO_searches')
        os.makedirs(scraped_links_dir, exist_ok=True)

        scraped_links_file_path = os.path.join(scraped_links_dir, f"{'_'.join(search_terms)}.txt")

        if os.path.exists(scraped_links_file_path):
            with open(scraped_links_file_path, 'r') as file:
                scraped_links = file.read().splitlines()
        else:
            scraped_links = []

        article_links = []
        while len(article_links) < retmax:
            new_links = driver.find_elements(By.CSS_SELECTOR, 'div.so-article-list-item > div > h3 > a')
            new_links = [link.get_attribute('href') for link in new_links if
                         link.get_attribute('href') not in scraped_links]

            article_links.extend(new_links)

            try:
                load_more_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.so--tall'))
                )
                driver.execute_script("arguments[0].click();", load_more_button)
                time.sleep(2)
            except TimeoutException:
                print("No more results to load or couldn't find 'Load More' button.")
                break
            except StaleElementReferenceException:
                print("No more results to load.")
                break
            except Exception as other:
                print(f"An unknown exception occurred, please let the dev know: {other}")
                break

        start_time = time.time()
        pbar = tqdm(total=retmax, dynamic_ncols=True)
        count = 0
        scraped_files = []
        failed_articles = []

        for link in article_links:
            if count >= retmax:
                break

            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[-1])

            try:
                driver.get(link)

                try:
                    pdf_link_element = wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, '#id2e > li:nth-child(1) > a:nth-child(1)')))
                    pdf_link = pdf_link_element.get_attribute('href')
                except (TimeoutException, NoSuchElementException):
                    print(f"PDF link not found for article: {link}")
                    failed_articles.append(link)
                    continue

                try:
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    doi_element = soup.find('meta', attrs={'name': 'citation_doi'})
                    if doi_element is not None:
                        doi = doi_element.get('content')
                        encoded_doi = quote(doi, safe='')
                    else:
                        print(f"DOI not found for article: {link}")
                        failed_articles.append(link)
                        continue
                except Exception as e:
                    print(f"Error occurred while extracting DOI for article: {link}")
                    print(f"Error: {e}")
                    failed_articles.append(link)
                    continue

                try:
                    pdf_response = requests.get(pdf_link)
                    filename = f"SO_{encoded_doi}.pdf"
                    with open(os.path.join(os.getcwd(), 'scraped_docs', filename), 'wb') as f:
                        f.write(pdf_response.content)

                    count += 1
                    elapsed_time = time.time() - start_time
                    avg_time_per_pdf = elapsed_time / count
                    est_time_remaining = avg_time_per_pdf * (retmax - count)
                    pbar.set_description(
                        f"DOI: {doi}, Count: {count}/{retmax}, Avg time per PDF: {avg_time_per_pdf:.2f}s, Est. time remaining: {est_time_remaining:.2f}s")
                    pbar.update(1)

                    scraped_files.append(filename)

                    with open(scraped_links_file_path, 'a') as file:
                        file.write(f"{link}\n")
                except Exception as e:
                    print(f"Error occurred while downloading PDF for article: {link}")
                    print(f"Error: {e}")
                    failed_articles.append(link)
            except Exception as e:
                print(f"Error occurred while processing article: {link}")
                print(f"Error: {e}")
                failed_articles.append(link)
            finally:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])

        driver.quit()
        pbar.close()

        print(f"Scraping completed. Successfully scraped {count} articles.")
        print(f"Failed to scrape {len(failed_articles)} articles.")
        print("Failed articles:")
        for article in failed_articles:
            print(article)

        return scraped_files

    except TimeoutException:
        print(
            "Timeout occurred while loading ScienceOpen. This could be due to slow internet connection or the website being down.")
        print("Skipping ScienceOpen scraping for this search term.")
        if driver:
            driver.quit()
        return []
    except Exception as e:
        print(f"An unexpected error occurred while scraping ScienceOpen: {e}")
        print("Skipping ScienceOpen scraping for this search term.")
        if driver:
            driver.quit()
        return []

def arxiv_search(search_terms, retmax, repository):
    print(f"Starting {repository} search with terms: {search_terms}")
    query = "+AND+".join(search_terms).replace(' ', '%20')
    print(f"Constructed query: {query}")

    tracking_filename = os.path.join(os.getcwd(), 'search_info', 'arXiv',
                                     f"{repository}_{'_'.join(search_terms)}_count.txt")
    fetched = 0
    if os.path.exists(tracking_filename):
        with open(tracking_filename, 'r') as f:
            fetched = int(f.read().strip())
    else:
        fetched = 0
        os.makedirs(os.path.join(os.getcwd(), 'search_info', 'arXiv'), exist_ok=True)

    print(f"Starting fetch from count: {fetched}")

    MAX_RETRIES = 3
    SEARCH_MAX_RETRIES = 10
    scraped_files = []

    while fetched < retmax:
        current_max = min(100, retmax - fetched)

        if repository == 'arxiv':
            api_url = f"https://export.arxiv.org/api/query?search_query=all:{query}&start={fetched}&max_results={current_max}"
        elif repository == 'chemrxiv':
            api_url = f"https://chemrxiv.org/engage/chemrxiv/public-api/v1/items?term={query}&skip={fetched}&limit={current_max}"
        else:
            print(f"Unsupported repository: {repository}")
            return []

        print(f"Fetching results from: {api_url}")

        search_retry_count = 0
        search_successful = False

        while search_retry_count < SEARCH_MAX_RETRIES and not search_successful:
            try:
                print(f"Sending request to API... (Retry: {search_retry_count})")
                response = requests.get(api_url)
                print(f"API response status code: {response.status_code}")
                response.raise_for_status()

                # print(f"Raw API response for {repository}:")
                # print(response.text[:1000])  # Print first 1000 characters of the response

                if repository == 'arxiv':
                    xml_data = response.text
                    soup = BeautifulSoup(xml_data, "xml")
                    entries = soup.find_all("entry")
                elif repository == 'chemrxiv':
                    json_data = response.json()
                    entries = json_data.get('itemHits', [])

                print(f"Number of entries found: {len(entries)}")

                if not entries:
                    print("No more results found. Exiting.")
                    break

                for index, entry in enumerate(entries, start=1):
                    print(f"Processing entry {index} out of {len(entries)}...")
                    retry_count = 0
                    download_successful = False

                    while retry_count < MAX_RETRIES and not download_successful:
                        try:
                            if repository == 'arxiv':
                                pdf_link = entry.find('link', {'title': 'pdf'})['href']
                                arxiv_id = entry.find('id').text.split('/')[-1]
                                doi = f"{arxiv_id}"
                            elif repository == 'chemrxiv':
                                pdf_link = entry['item']['asset']['original']['url']
                                doi = entry['item'].get('doi', '')

                            print(f"PDF link found: {pdf_link}")

                            if pdf_link:
                                pdf_response = requests.get(pdf_link)
                                pdf_response.raise_for_status()
                                pdf_content = pdf_response.content

                                filename = f"{repository}_{doi_to_filename(doi)}.pdf"

                                with open(os.path.join(os.getcwd(), 'scraped_docs', filename), 'wb') as f:
                                    f.write(pdf_content)

                                scraped_files.append(filename)
                                fetched += 1
                                download_successful = True

                                with open(tracking_filename, 'w') as f:
                                    f.write(str(fetched))

                                print(f"Successfully downloaded PDF for entry {index}.")
                            else:
                                print(f"No PDF link found for entry {index}. Skipping.")
                                download_successful = True  # Mark as successful to move to next entry

                        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
                            retry_count += 1
                            print(
                                f"Error downloading PDF for entry {index}. Retry attempt {retry_count}/{MAX_RETRIES}. Error: {e}")
                            time.sleep(10)

                    if not download_successful:
                        print(
                            f"Failed to download entry {index} after {MAX_RETRIES} attempts. Skipping this entry.")

                    if fetched >= retmax:
                        print(f"Reached retmax of {retmax}. Stopping search.")
                        break

                if fetched == 0:
                    print("No new entries fetched. Breaking loop.")
                    break

                search_successful = True

            except (requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
                search_retry_count += 1
                print(
                    f"Failed to fetch search results. Retry attempt {search_retry_count}/{SEARCH_MAX_RETRIES}. Error: {e}")
                time.sleep(10)

        if not search_successful or not entries or fetched >= retmax:
            break

    print(f"{repository} search completed. Total files scraped: {len(scraped_files)}")
    return scraped_files

def pubmed_search(search_terms, retmax):
    query = " AND ".join(search_terms)

    esearch_params = {
        'db': 'pmc',
        'term': query,
        'retmode': 'json',
        'retmax': retmax
    }

    print("Now performing esearch...")
    try:
        esearch_response = requests.get(ESEARCH_URL, params=esearch_params)
        esearch_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []

    esearch_data = esearch_response.json()

    if 'esearchresult' in esearch_data:
        uid_list = esearch_data['esearchresult']['idlist']

        if not uid_list:
            print("No search results found.")
            return []

        downloaded_files = os.listdir(os.path.join(os.getcwd(), 'scraped_docs'))
        downloaded_files = [file.replace("pubmed_", "").replace(".xml", "") for file in downloaded_files if
                            "pubmed_" in file]

        # Count only the downloaded files that are part of this search
        downloaded_from_current_search = [uid for uid in uid_list if uid in downloaded_files]
        num_downloaded = len(downloaded_from_current_search)

        print(f"{num_downloaded} files already downloaded for this search.")

        scraped_files = []

        for uid in uid_list:
            if uid not in downloaded_files:
                if num_downloaded >= retmax:
                    print("Reached maximum number of downloads for this search. Stopping.")
                    return scraped_files

                efetch_params = {
                    'db': 'pmc',
                    'id': uid,
                    'retmode': 'xml',
                    'rettype': 'full'
                }

                print(f"Now performing efetch for UID {uid}...")
                try:
                    efetch_response = requests.get(EFETCH_URL, params=efetch_params)
                    efetch_response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Request failed: {e}")
                    continue

                xml_data = efetch_response.text
                root = ET.fromstring(xml_data)

                # Check if full-text is available
                if root.find(".//body"):
                    filename = f"pubmed_{uid}.xml"
                    with open(os.path.join(os.getcwd(), 'scraped_docs', filename), 'w') as f:
                        f.write(xml_data)
                    num_downloaded += 1
                    scraped_files.append(filename)
                else:
                    print(f"Full text not available for UID {uid}. Skipping.")

                time.sleep(1 / 2)

        return scraped_files

def read_api_count():
    try:
        with open(os.path.join(os.getcwd(), 'search_info', 'unpaywall', 'api_call_count.txt'), "r") as f:
            data = f.read().split("\n")
            date = data[0]
            count = int(data[1])
        return date, count
    except FileNotFoundError:
        return None, 0

def write_api_count(date, count):
    with open(os.path.join(os.getcwd(), 'search_info', 'unpaywall', 'api_call_count.txt'), "w") as f:
        f.write(f"{date}\n{count}")

def read_last_state():
    try:
        with open(os.path.join(os.getcwd(), 'search_info', 'unpaywall', 'last_state.txt'), "r") as f:
            data = json.load(f)
        return data['last_chunk'], data['last_page']
    except FileNotFoundError:
        return None, 1

def write_last_state(last_chunk, last_page):
    with open(os.path.join(os.getcwd(), 'search_info', 'unpaywall', 'last_state.txt'), "w") as f:
        json.dump({'last_chunk': last_chunk, 'last_page': last_page}, f)

def download_pdf(url, doi):
    try:
        pdf_response = requests.get(url)
        pdf_response.raise_for_status()
        filename = f"unpaywall_{doi_to_filename(doi)}.pdf"
        with open(os.path.join(os.getcwd(), 'scraped_docs', filename), 'wb') as f:
            f.write(pdf_response.content)
        return filename
    except requests.exceptions.RequestException as e:
        print(f"PDF download failed: {e}")
        return None

def unpaywall_search(query_chunks, retmax, email):
    last_date, api_count = read_api_count()
    today = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(os.path.join(os.getcwd(), 'search_info', 'unpaywall'), exist_ok=True)

    if last_date != today:
        api_count = 0

    if api_count >= 100000:
        print("Reached daily API call limit. Try again tomorrow.")
        return []

    last_chunk, last_page = read_last_state()

    resume = False if last_chunk is None else True
    scraped_files = []
    total_downloaded = 0

    for chunk in query_chunks:
        if resume and chunk != last_chunk:
            continue
        query = " AND ".join(chunk)
        page = last_page if resume and chunk == last_chunk else 1

        while total_downloaded <= retmax and api_count < 100000:
            unpaywall_params = {
                'query': query,
                'is_oa': 'true',
                'email': email,
                'page': page
            }

            print(f"Now scraping Unpaywall for {chunk}, page {page}")

            try:
                unpaywall_response = requests.get("https://api.unpaywall.org/v2/search", params=unpaywall_params)
                unpaywall_response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                return scraped_files

            api_count += 1
            if api_count >= 100000:
                print("Reached daily API call limit. Exiting.")
                write_api_count(today, api_count)
                write_last_state(chunk, page)
                return scraped_files

            write_api_count(today, api_count)

            unpaywall_data = json.loads(unpaywall_response.text)

            if 'results' not in unpaywall_data:
                print("No results found in the Unpaywall API response.")
                break

            doi_list = [result['response']['doi'] for result in unpaywall_data['results'] if
                        'response' in result and 'doi' in result['response']]

            if not doi_list:
                print("No DOIs found in the Unpaywall API response.")
                break

            downloaded_files = os.listdir(os.path.join(os.getcwd(), 'scraped_docs'))

            for doi in doi_list:
                if total_downloaded >= retmax:
                    print(f"Reached retmax of {retmax}. Stopping search.")
                    return scraped_files

                if doi not in downloaded_files:
                    print(f"Now fetching data for DOI {doi}...")
                    try:
                        doi_response = requests.get(f"https://api.unpaywall.org/v2/{doi}?email={email}")
                        doi_response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Request failed: {e}")
                        continue

                    api_count += 1
                    if api_count >= 100000:
                        print("Reached daily API call limit. Exiting.")
                        write_api_count(today, api_count)
                        write_last_state(chunk, page)
                        return scraped_files

                    write_api_count(today, api_count)

                    doi_data = doi_response.json()

                    if doi_data.get('is_oa'):
                        pdf_url = doi_data['best_oa_location']['url_for_pdf']
                        if pdf_url:
                            pdf_filename = download_pdf(pdf_url, doi)
                            if pdf_filename:
                                scraped_files.append(pdf_filename)
                                total_downloaded += 1
                        else:
                            print(f"No PDF URL found for DOI {doi}")

                    doi_data_str = json.dumps(doi_data, indent=4)
                    json_filename = f"unpaywall_{doi_to_filename(doi)}.json"
                    with open(os.path.join(os.getcwd(), 'scraped_docs', json_filename), 'w') as f:
                        f.write(doi_data_str)
                    scraped_files.append(json_filename)
                    total_downloaded += 1

                    time.sleep(1 / 10)

            page += 1
            write_last_state(chunk, page)

    return scraped_files