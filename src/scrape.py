import sqlite3
import sys
import os
import requests
from tqdm import tqdm
import time
import json
import logging
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup
from selenium.common import TimeoutException
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import xml.etree.ElementTree as ET
from src.extract import extract
from src.utils import (get_out_id, get_chrome_driver, doi_to_filename, is_file_processed)

# Constants
CONVERT_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={}&format=json"
EUTILS_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={}"
ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
repositories = ['arxiv', 'chemrxiv']  # Decided to remove bio and med, as their api's are not very good


# I could be convinced to add them back, but because the api doesn't allow for search terms, I would need to write code
# to build a local database and search that, which would be time-consuming and a hassle for the end user.

def scrape_scienceopen(search_terms, retmax, concurrent=False, schema_file=None, user_instructions=None, model_name_version=None):
    """
    Scrape articles from ScienceOpen based on search terms, with optional concurrent extraction.

    Args:
    search_terms (list): A list of search terms to use on ScienceOpen.
    retmax (int): The maximum number of articles to scrape.
    concurrent (bool): Whether to perform extraction concurrently with downloading.
    schema_file (str): Path to the schema file for extraction (required if concurrent is True).
    user_instructions (str): Instructions for the extraction model (required if concurrent is True).
    model_name_version (str): Name and version of the model to use for extraction (required if concurrent is True).

    Returns:
    list: A list of filenames of the scraped PDFs.
    """
    if concurrent and (schema_file is None or user_instructions is None or model_name_version is None):
        raise ValueError("schema_file, user_instructions, and model_name_version must be provided when concurrent is True")

    # Split model name and version
    try:
        model_name, model_version = model_name_version.split(':')
    except ValueError:
        model_name = model_name_version
        model_version = 'latest'

    csv_file = os.path.join(os.getcwd(), 'results',
                            f"{model_name}_{model_version}_{os.path.splitext(schema_file)[0].split('/')[-1]}.csv")

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

                filename = f"SO_{encoded_doi}.pdf"
                file_path = os.path.join(os.getcwd(), 'scraped_docs', filename)

                if os.path.exists(file_path):
                    print(f"{filename} already exists.")
                    if concurrent and not is_file_processed(csv_file, filename):
                        print(f"{filename} not extracted for this task; performing extraction...")
                        try:
                            extracted_data = extract(file_path, schema_file, model_name_version, user_instructions)
                            if extracted_data:
                                print(f"Successfully extracted data from {filename}")
                            else:
                                print(f"Failed to extract data from {filename}")
                        except Exception as e:
                            print(f"Error extracting data from {filename}: {e}")
                    continue

                try:
                    pdf_response = requests.get(pdf_link)
                    with open(file_path, 'wb') as f:
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

                    if concurrent:
                        try:
                            extracted_data = extract(file_path, schema_file, model_name_version, user_instructions)
                            if extracted_data:
                                print(f"Successfully extracted data from {filename}")
                            else:
                                print(f"Failed to extract data from {filename}")
                        except Exception as e:
                            print(f"Error extracting data from {filename}: {e}")

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


def arxiv_search(search_terms, retmax, repository, concurrent=False, schema_file=None, user_instructions=None, model_name_version=None):
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
    if concurrent and (schema_file is None or user_instructions is None or model_name_version is None):
        raise ValueError("schema_file, user_instructions, and model_name_version must be provided when concurrent is True")

    # Split model name and version
    try:
        model_name, model_version = model_name_version.split(':')
    except ValueError:
        model_name = model_name_version
        model_version = 'latest'

    csv_file = os.path.join(os.getcwd(), 'results',
                            f"{model_name}_{model_version}_{os.path.splitext(schema_file)[0].split('/')[-1]}.csv")

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

    while fetched < retmax:
        current_max = min(50, retmax - fetched)

        # Construct API URL based on repository
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
                        if concurrent and not is_file_processed(csv_file, filename):
                            print(f"{filename} not extracted for this task; performing extraction...")
                            try:
                                extracted_data = extract(file_path, schema_file, model_name_version, user_instructions)
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

    # Load the list of PMIDs without full text
    no_fulltext_file = os.path.join(os.getcwd(), 'search_info', 'no_fulltext.txt')
    if os.path.exists(no_fulltext_file):
        with open(no_fulltext_file, 'r') as f:
            no_fulltext_pmids = set(f.read().splitlines())
    else:
        no_fulltext_pmids = set()

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
            if uid in no_fulltext_pmids:
                print(f"Skipping UID {uid} as it was previously found to have no full text available.")
                continue

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
                    print(f"Full text not available for UID {uid}. Adding to no_fulltext.txt and skipping.")
                    with open(no_fulltext_file, 'a') as f:
                        f.write(f"{uid}\n")
                    no_fulltext_pmids.add(uid)

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


def read_api_count():
    """
    Read the current API call count for Unpaywall.

    Returns:
    tuple: (date, count) of the last API call count.
    """
    try:
        with open(os.path.join(os.getcwd(), 'search_info', 'unpaywall', 'api_call_count.txt'), "r") as f:
            data = f.read().split("\n")
            date = data[0]
            count = int(data[1])
        return date, count
    except FileNotFoundError:
        return None, 0


def write_api_count(date, count):
    """
    Write the current API call count for Unpaywall.

    Args:
    date (str): Current date.
    count (int): Current API call count.
    """
    with open(os.path.join(os.getcwd(), 'search_info', 'unpaywall', 'api_call_count.txt'), "w") as f:
        f.write(f"{date}\n{count}")


def read_last_state():
    """
    Read the last state of Unpaywall search.

    Returns:
    tuple: (last_chunk, last_page) of the last search state.
    """
    try:
        with open(os.path.join(os.getcwd(), 'search_info', 'unpaywall', 'last_state.txt'), "r") as f:
            data = json.load(f)
        return data['last_chunk'], data['last_page']
    except FileNotFoundError:
        return None, 1


def write_last_state(last_chunk, last_page):
    """
    Write the current state of Unpaywall search.

    Args:
    last_chunk (list): Last processed search chunk.
    last_page (int): Last processed page number.
    """
    with open(os.path.join(os.getcwd(), 'search_info', 'unpaywall', 'last_state.txt'), "w") as f:
        json.dump({'last_chunk': last_chunk, 'last_page': last_page}, f)


def download_pdf(url, doi):
    """
    Download a PDF from a given URL.

    Args:
    url (str): URL of the PDF.
    doi (str): DOI of the paper.

    Returns:
    str: Filename of the downloaded PDF, or None if download failed.
    """
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


def unpaywall_search(query_chunks, retmax, email, concurrent=False, schema_file=None, user_instructions=None, model_name_version=None):
    """
    Search and download papers from Unpaywall API, with optional concurrent extraction.

    Args:
    query_chunks (list): List of search term chunks to query.
    retmax (int): Maximum number of results to retrieve.
    email (str): Email address for Unpaywall API authentication.
    concurrent (bool): Whether to perform extraction concurrently with downloading.
    schema_file (str): Path to the schema file for extraction (required if concurrent is True).
    user_instructions (str): Instructions for the extraction model (required if concurrent is True).
    model_name_version (str): Name and version of the model to use for extraction (required if concurrent is True).

    Returns:
    list: List of filenames of downloaded papers and JSON metadata.
    """
    if concurrent and (schema_file is None or user_instructions is None or model_name_version is None):
        raise ValueError("schema_file, user_instructions, and model_name_version must be provided when concurrent is True")

    # Split model name and version
    try:
        model_name, model_version = model_name_version.split(':')
    except ValueError:
        model_name = model_name_version
        model_version = 'latest'

    csv_file = os.path.join(os.getcwd(), 'results',
                            f"{model_name}_{model_version}_{os.path.splitext(schema_file)[0].split('/')[-1]}.csv")

    # Read the last API call count and date
    last_date, api_count = read_api_count()
    today = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(os.path.join(os.getcwd(), 'search_info', 'unpaywall'), exist_ok=True)

    # Reset API count if it's a new day
    if last_date != today:
        api_count = 0

    # Check if daily API limit is reached
    if api_count >= 100000:
        print("Reached daily API call limit. Try again tomorrow.")
        return []

    # Read the last search state for resuming interrupted searches
    last_chunk, last_page = read_last_state()

    resume = False if last_chunk is None else True
    scraped_files = []
    total_downloaded = 0

    # Iterate through search term chunks
    for chunk in query_chunks:
        if resume and chunk != last_chunk:
            continue
        query = " AND ".join(chunk)
        page = last_page if resume and chunk == last_chunk else 1

        # Continue searching while within limits
        while total_downloaded <= retmax and api_count < 100000:
            unpaywall_params = {
                'query': query,
                'is_oa': 'true',
                'email': email,
                'page': page
            }

            print(f"Now scraping Unpaywall for {chunk}, page {page}")

            # Make API request
            try:
                unpaywall_response = requests.get("https://api.unpaywall.org/v2/search", params=unpaywall_params)
                unpaywall_response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                return scraped_files

            # Update API call count
            api_count += 1
            if api_count >= 100000:
                print("Reached daily API call limit. Exiting.")
                write_api_count(today, api_count)
                write_last_state(chunk, page)
                return scraped_files

            write_api_count(today, api_count)

            unpaywall_data = json.loads(unpaywall_response.text)

            # Check if results are present
            if 'results' not in unpaywall_data:
                print("No results found in the Unpaywall API response.")
                break

            # Extract DOIs from the response
            doi_list = [result['response']['doi'] for result in unpaywall_data['results'] if
                        'response' in result and 'doi' in result['response']]

            if not doi_list:
                print("No DOIs found in the Unpaywall API response.")
                break

            # Process each DOI
            for doi in doi_list:
                if total_downloaded >= retmax:
                    print(f"Reached retmax of {retmax}. Stopping search.")
                    return scraped_files

                pdf_filename = f"unpaywall_{doi_to_filename(doi)}.pdf"
                json_filename = f"unpaywall_{doi_to_filename(doi)}.json"
                pdf_path = os.path.join(os.getcwd(), 'scraped_docs', pdf_filename)
                json_path = os.path.join(os.getcwd(), 'scraped_docs', json_filename)

                # Check if files already exist
                pdf_exists = os.path.exists(pdf_path)
                json_exists = os.path.exists(json_path)

                if pdf_exists or json_exists:
                    print(f"Files for DOI {doi} already exist.")
                    if concurrent and not is_file_processed(csv_file, pdf_filename):
                        print(f"{pdf_filename} not extracted for this task; performing extraction...")
                        try:
                            extracted_data = extract(pdf_path, schema_file, model_name_version, user_instructions)
                            if extracted_data:
                                print(f"Successfully extracted data from {pdf_filename}")
                            else:
                                print(f"Failed to extract data from {pdf_filename}")
                        except Exception as e:
                            print(f"Error extracting data from {pdf_filename}: {e}")
                    continue

                print(f"Now fetching data for DOI {doi}...")
                try:
                    doi_response = requests.get(f"https://api.unpaywall.org/v2/{doi}?email={email}")
                    doi_response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Request failed: {e}")
                    continue

                # Update API call count
                api_count += 1
                if api_count >= 100000:
                    print("Reached daily API call limit. Exiting.")
                    write_api_count(today, api_count)
                    write_last_state(chunk, page)
                    return scraped_files

                write_api_count(today, api_count)

                doi_data = doi_response.json()

                # Download PDF if available
                if doi_data.get('is_oa'):
                    pdf_url = doi_data['best_oa_location']['url_for_pdf']
                    if pdf_url:
                        pdf_filename = download_pdf(pdf_url, doi)
                        if pdf_filename:
                            scraped_files.append(pdf_filename)
                            total_downloaded += 1

                            if concurrent:
                                try:
                                    extracted_data = extract(pdf_path, schema_file, model_name_version, user_instructions)
                                    if extracted_data:
                                        print(f"Successfully extracted data from {pdf_filename}")
                                    else:
                                        print(f"Failed to extract data from {pdf_filename}")
                                except Exception as e:
                                    print(f"Error extracting data from {pdf_filename}: {e}")
                    else:
                        print(f"No PDF URL found for DOI {doi}")

                # Save metadata as JSON
                doi_data_str = json.dumps(doi_data, indent=4)
                with open(json_path, 'w') as f:
                    f.write(doi_data_str)
                scraped_files.append(json_filename)
                total_downloaded += 1

                time.sleep(1 / 10)  # Rate limiting

            page += 1
            write_last_state(chunk, page)

    return scraped_files

def scrape(args):
    """
    Main function to scrape papers from various sources based on user input or provided arguments.

    This function coordinates the scraping process across multiple repositories including
    PubMed, arXiv, ChemRxiv, ScienceOpen, Unpaywall, and a custom database if specified.
    It handles user input for search terms and options when not in automatic mode.

    Args:
    args (dict): A dictionary containing scraping parameters. If empty or None, user input is requested.

    Returns:
    None: The function writes scraped file information to a text file and doesn't return any value.
    """
    # Extract arguments or set to None if not provided
    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')
    pubmedyn = args.get('pubmedyn')
    arxivyn = args.get('arxivyn')
    soyn = args.get('ScienceOpenyn')
    customdb = args.get('customdb')
    auto = args.get('auto')
    retmax = args.get('retmax')
    base_url = args.get('base_url')
    upwyn = args.get('Unpaywallyn')
    email = args.get('Email')

    # If not in automatic mode, get user inputs
    if auto is None:
        # Get 'definitely contains' search terms from user
        def_search_terms = input(
            "Enter 'definitely contains' search terms (comma separated) or type 'None' to only use maybe search terms: ").lower().split(
            ',')

        # Get 'maybe contains' search terms from user
        maybe_search_terms = input(
            "Enter 'maybe contains' search terms (comma separated) or type 'None' to only use definite search terms: ").lower().split(
            ',')

        # Define maximum returned papers per search term
        while True:
            try:
                retmax = int(input("Set the maximum number of papers to fetch per search:"))
                if retmax < 1:
                    print("Please enter a positive integer.")
                else:
                    break
            except ValueError:
                print("Please enter a valid number.")

        # Get user preferences for different repositories
        pubmedyn = input("Would you like to search pubmed?(y/n)").lower()
        arxivyn = input("Would you like to search through the arxivs?(y/n)").lower()
        soyn = input("Would you like to scrape ScienceOpen?(y/n):").lower()
        upwyn = input("Would you like to scrape unpwaywall?(y/n):").lower()

        if upwyn == 'y':
            email = input("Enter email for use with unpaywall:").lower()

        customdb = input("Would you like to search and download from a custom database?(y/n):").lower()

        if customdb == 'y':
            base_url = input("Enter base url:")

    # Generate output directory ID and query chunks
    output_directory_id, query_chunks = get_out_id(def_search_terms, maybe_search_terms)

    # Create necessary directories
    os.makedirs(os.path.join(os.getcwd(), 'scraped_docs'), exist_ok=True)
    os.makedirs(os.path.join(os.getcwd(), 'search_info'), exist_ok=True)

    # Define the search info file path
    search_info_file = os.path.join(os.getcwd(), 'search_info', f"{output_directory_id}.txt")

    # Scrape from PubMed if selected
    if pubmedyn == "y":
        for chunk in query_chunks:
            print("Current search: " + str(chunk))
            scraped_files = pubmed_search(chunk, retmax)
            with open(search_info_file, 'a') as f:
                f.write('\n'.join(scraped_files) + '\n')

    # Scrape from arXiv and ChemRxiv if selected
    if arxivyn == "y":
        for repository in repositories:
            for chunk in query_chunks:
                print("Current search: " + str(chunk))
                scraped_files = arxiv_search(chunk, retmax, repository)
                if scraped_files is not None:
                    with open(search_info_file, 'a') as f:
                        f.write('\n'.join(scraped_files) + '\n')

    # Scrape from ScienceOpen if selected
    if soyn == "y":
        for chunk in query_chunks:
            print(f"Now running ScienceOpen scrape for {chunk}")
            scraped_files = scrape_scienceopen(chunk, retmax)
            with open(search_info_file, 'a') as f:
                f.write('\n'.join(scraped_files) + '\n')

    # Scrape from Unpaywall if selected
    if upwyn == "y":
        scraped_files = unpaywall_search(query_chunks, retmax, email)
        with open(search_info_file, 'a') as f:
            f.write('\n'.join(scraped_files) + '\n')

    # Scrape from custom database if selected
    if customdb == "y":
        # Create a connection to the SQLite database
        conn = sqlite3.connect(str(os.getcwd()) + '/customdb/metadata.db')
        c = conn.cursor()

        scraped_files = []

        # Iterate over all search terms
        for chunk in query_chunks:
            print(f"Current search: {chunk}")

            # Create the SQL query
            query = 'SELECT * FROM metadata WHERE '
            query += ' AND '.join([f'title LIKE ?' for _ in chunk])
            query += ' ORDER BY title LIMIT ?'

            # Create the parameters for the SQL query
            params = tuple([f'%{term}%' for term in chunk] + [retmax])

            # Execute the SQL query
            c.execute(query, params)

            # Fetch all the results
            results = c.fetchall()
            print(f"Found {len(results)} results for search terms: {chunk}")

            # Iterate over the results
            for result in results:
                # Extract and format the DOI
                doi = result[0]
                encoded_doi = quote(doi, safe='')
                print(f"Processing DOI: {doi}")

                # Create the URL for the paper
                url = base_url + quote(doi)
                print(f"URL: {url}")

                # Get the webpage content
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Request failed: {e}")
                    continue

                # Check if the request was successful
                if response.status_code == 200:
                    # Parse the HTML
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Find the PDF link
                    pdf_link = None
                    for button in soup.find_all('button'):
                        if '.pdf' in button.get('onclick', ''):
                            pdf_link = 'https:' + button['onclick'].split("'")[1]
                            break

                    if pdf_link is not None:
                        # Download the PDF
                        try:
                            pdf_response = requests.get(pdf_link)
                            pdf_response.raise_for_status()
                        except requests.exceptions.RequestException as e:
                            print(f"Request failed: {e}")
                            continue

                        if pdf_response.status_code == 200:
                            # Extract the PDF from the response
                            pdf = pdf_response.content

                            # Create the path for the PDF file
                            pdf_filename = f'{encoded_doi}.pdf'
                            pdf_path = os.path.join(os.getcwd(), 'scraped_docs', pdf_filename)
                            print(f"Saving PDF to: {pdf_path}")

                            # Write the PDF to a file
                            with open(pdf_path, 'wb') as f:
                                f.write(pdf)

                            scraped_files.append(pdf_filename)
                        else:
                            print(f"Failed to download PDF. Status code: {pdf_response.status_code}")
                    else:
                        print("No PDF link found on the page. Saving the webpage as HTML.")

                        # Create the path for the HTML file
                        html_filename = f'{doi}.html'
                        html_path = os.path.join(os.getcwd(), 'scraped_docs', html_filename)
                        print(f"Saving HTML to: {html_path}")

                        # Write the HTML to a file
                        with open(html_path, 'w') as f:
                            f.write(response.text)

                        scraped_files.append(html_filename)
                else:
                    print(f"Failed to access the webpage. Status code: {response.status_code}")

        # Write the scraped files to the search info file
        with open(search_info_file, 'a') as f:
            f.write('\n'.join(scraped_files) + '\n')

        # Close the connection to the database
        conn.close()

    # If not in automatic mode, restart the script
    if auto is None:
        python = sys.executable
        os.execl(python, python, *sys.argv)

    # If in automatic mode, return None
    else:
        return None
