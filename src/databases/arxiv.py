import os
import requests
import time
from bs4 import BeautifulSoup
from src.extract import extract
from src.utils import (doi_to_filename, is_file_processed)
from src.classes import JobSettings

# Constants
repositories = ['arxiv', 'chemrxiv']  # Decided to remove bio and med, as their api's are not very good
# I could be convinced to add them back, but because the api doesn't allow for search terms, I would need to write code
# to build a local database and search that, which would be time-consuming and a hassle for the end user.

def arxiv_search(job_settings:JobSettings, search_terms, repository): # retmax, concurrent=False, schema_file=None, user_instructions=None, model_name_version=None
    """
    Search and download papers from arXiv or ChemRxiv repositories, with optional concurrent extraction.

    Args:
    search_terms (list): List of search terms.
    retmax (int): Maximum number of results to retrieve.
    repository (str): Either 'arxiv' or 'chemrxiv'.
    concurrent (bool): Whether to perform extraction concurrently with downloading.
    schema_file (str): Path to the schema file for extraction (required if concurrent is True).
    user_instructions (str): Instructions for the extraction model (required if concurrent is True).
    model_name_version (str): Name and version of the model to use for extraction (required if concurrent is True).

    Returns:
    list: List of filenames of downloaded papers.
    """
    if job_settings.concurrent and (job_settings.files.schema is None or job_settings.extract.user_instructions is None or job_settings.model_name_version is None):
        raise ValueError("schema_file, user_instructions, and model_name_version must be provided when concurrent is True")

    print(f"Starting {repository} search with terms: {search_terms}")
    query = "+AND+".join(search_terms).replace(' ', '%20')
    print(f"Constructed query: {query}")

    # Set up tracking file for resuming interrupted searches
    tracking_filename = os.path.join(os.getcwd(), 'search_info', 'arXiv',
                                     f"{repository}_{'_'.join(search_terms)}_count.txt")
    fetched = 0
    if os.path.exists(tracking_filename):
        with open(tracking_filename, 'r') as f:
            fetched = int(f.read().strip())
    else:
        os.makedirs(os.path.join(os.getcwd(), 'search_info', 'arXiv'), exist_ok=True)

    print(f"Starting fetch from count: {fetched}")

    MAX_RETRIES = 3
    SEARCH_MAX_RETRIES = 10
    scraped_files = []

    while fetched < job_settings.scrape.retmax:
        current_max = min(50, job_settings.scrape.retmax - fetched)

        # Construct API URL based on repository
        known_api_urls = {'arxiv': f"https://export.arxiv.org/api/query?search_query=all:{query}&start={fetched}&max_results={current_max}",
                          'chemrxiv': f"https://chemrxiv.org/engage/chemrxiv/public-api/v1/items?term={query}&skip={fetched}&limit={current_max}",
                          }
        if repository not in [x for x in known_api_urls.keys()]:
            print(f"Unsupported repository: {repository}")
            return []
        api_url = known_api_urls[repository]
        print(f"Fetching results from: {api_url}")

        search_retry_count = 0
        search_successful = False

        while search_retry_count < SEARCH_MAX_RETRIES and not search_successful:
            try:
                print(f"Sending request to API... (Retry: {search_retry_count})")
                response = requests.get(api_url)
                print(f"API response status code: {response.status_code}")
                response.raise_for_status()

                # Parse response based on repository
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

                # Process each entry
                for index, entry in enumerate(entries, start=1):
                    print(f"Processing entry {index} out of {len(entries)}...")
                    retry_count = 0
                    download_successful = False

                    # Extract PDF link and DOI based on repository
                    if repository == 'arxiv':
                        pdf_link = entry.find('link', {'title': 'pdf'})['href']
                        arxiv_id = entry.find('id').text.split('/')[-1]
                        doi = f"{arxiv_id}"
                    elif repository == 'chemrxiv':
                        pdf_link = entry['item']['asset']['original']['url']
                        doi = entry['item'].get('doi', '')

                    filename = f"{repository}_{doi_to_filename(doi)}.pdf"
                    file_path = os.path.join(os.getcwd(), 'scraped_docs', filename)

                    if os.path.exists(file_path):
                        print(f"{filename} already downloaded.")
                        if job_settings.concurrent and not is_file_processed(job_settings.files.csv, filename):
                            print(f"{filename} not extracted for this task; performing extraction...")
                            try:
                                extracted_data = extract(file_path, job_settings.files.schema, job_settings.model_name_version, job_settings.extract.user_instructions)
                                if extracted_data:
                                    print(f"Successfully extracted data from {filename}")
                                else:
                                    print(f"Failed to extract data from {filename}")
                            except Exception as e:
                                print(f"Error extracting data from {filename}: {e}")
                        continue

                    while retry_count < MAX_RETRIES and not download_successful:
                        try:
                            print(f"PDF link found: {pdf_link}")

                            if pdf_link:
                                # Download PDF
                                pdf_response = requests.get(pdf_link)
                                pdf_response.raise_for_status()
                                pdf_content = pdf_response.content

                                with open(file_path, 'wb') as f:
                                    f.write(pdf_content)

                                scraped_files.append(filename)
                                fetched += 1
                                download_successful = True

                                # Update tracking file
                                with open(tracking_filename, 'w') as f:
                                    f.write(str(fetched))

                                print(f"Successfully downloaded PDF for entry {index}.")

                                if job_settings.concurrent:
                                    try:
                                        extracted_data = extract(file_path, job_settings.files.schema, job_settings.model_name_version, job_settings.extract.user_instructions)
                                        if extracted_data:
                                            print(f"Successfully extracted data from {filename}")
                                        else:
                                            print(f"Failed to extract data from {filename}")
                                    except Exception as e:
                                        print(f"Error extracting data from {filename}: {e}")

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

                    if fetched >= job_settings.scrape.retmax:
                        print(f"Reached retmax of {job_settings.scrape.retmax}. Stopping search.")
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

        if not search_successful or not entries or fetched >= job_settings.scrape.retmax:
            break

    print(f"{repository} search completed. Total files scraped: {len(scraped_files)}")
    return scraped_files
