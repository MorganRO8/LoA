import os
import requests
import time
import xml.etree.ElementTree as ET
from src.extract import extract
from src.utils import is_file_processed

# Constants
ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'

def pubmed_search(search_terms, retmax, concurrent=False, schema_file=None, user_instructions=None, model_name_version=None):
    """
    Search and download papers from PubMed Central, with optional concurrent extraction.

    Args:
    search_terms (list): List of search terms.
    retmax (int): Maximum number of results to retrieve.
    concurrent (bool): Whether to perform extraction concurrently with downloading.
    schema_file (str): Path to the schema file for extraction (required if concurrent is True).
    user_instructions (str): Instructions for the extraction model (required if concurrent is True).
    model_name_version (str): Name and version of the model to use for extraction (required if concurrent is True).

    Returns:
    list: List of filenames of downloaded papers (and extracted data if concurrent is True).
    """
    if concurrent and (schema_file is None or user_instructions is None or model_name_version is None):
        raise ValueError("schema_file, user_instructions, and model_name_version must be provided when concurrent is True")

    # Split model name and version
    try:
        model_name, model_version = model_name_version.split(':')
    except ValueError:
        model_name = model_name_version
        model_version = 'latest'
        model_name_version = f"{model_name}:{model_version}"

    csv_file = os.path.join(os.getcwd(), 'results',
                            f"{model_name}_{model_version}_{os.path.splitext(schema_file)[0].split('/')[-1]}.csv")

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

        downloaded_from_current_search = [uid for uid in uid_list if uid in downloaded_files]
        num_downloaded = len(downloaded_from_current_search)

        print(f"{num_downloaded} files already downloaded for this search.")

        scraped_files = []

        for uid in uid_list:
            filename = f"pubmed_{uid}.xml"
            file_path = os.path.join(os.getcwd(), 'scraped_docs', filename)
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

                if root.find(".//body"):
                    with open(file_path, 'w') as f:
                        f.write(xml_data)
                    num_downloaded += 1
                    scraped_files.append(filename)

                    if concurrent:
                        try:
                            extracted_data = extract(file_path, schema_file, model_name_version, user_instructions)
                            if extracted_data:
                                print(f"Successfully extracted data from {filename}")
                            else:
                                print(f"Failed to extract data from {filename}")
                        except Exception as e:
                            print(f"Error extracting data from {filename}: {e}")
                else:
                    print(f"Full text not available for UID {uid}. Skipping.")

                time.sleep(1 / 2)

            elif (not is_file_processed(csv_file, filename)) and concurrent:
                print(f"{filename} already downloaded, but not extracted from for this task; performing extraction...")
                try:
                    extracted_data = extract(file_path, schema_file, model_name_version, user_instructions)
                    if extracted_data:
                        print(f"Successfully extracted data from {filename}")
                    else:
                        print(f"Failed to extract data from {filename}")
                except Exception as e:
                    print(f"Error extracting data from {filename}: {e}")

        return scraped_files
    else:
        print("No 'esearchresult' in the API response.")
        return []
