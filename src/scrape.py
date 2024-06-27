import concurrent.futures
import glob
import logging
import os
import sqlite3
import sys
import time
import json
from urllib.parse import quote, urljoin
import pkg_resources
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from src.utils import get_out_id, truncate_filename
import xml.etree.ElementTree as ET

# Constants
CONVERT_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={}&format=json"
EUTILS_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={}"
ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
repositories = ['arxiv', 'medrxiv', 'biorxiv', 'chemrxiv']


def scrape(args):
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

    # Get user inputs if need be

    if auto is None:
        # get 'definitely contains' search terms from user
        def_search_terms = input(
            "Enter 'definitely contains' search terms (comma separated) or type 'None' to only use maybe search terms: ").lower().split(
            ',')

        # get 'maybe contains' search terms from user
        maybe_search_terms = input(
            "Enter 'maybe contains' search terms (comma separated) or type 'None' to only use definite search terms: ").lower().split(
            ',')

        # define maximum returned papers per search term
        while True:
            try:
                retmax = int(input("Set the maximum number of papers to fetch per search:"))
                if retmax < 1:
                    print("Please enter a positive integer.")
                else:
                    break
            except ValueError:
                print("Please enter a valid number.")

        pubmedyn = input("Would you like to search pubmed?(y/n)").lower()

        arxivyn = input("Would you like to search through the arxivs?(y/n)").lower()

        soyn = input("Would you like to scrape ScienceOpen?(y/n):").lower()

        upwyn = input("Would you like to scrape unpwaywall?(y/n):").lower()

        if upwyn == 'y':
            email = input("Enter email for use with unpaywall:").lower()

        customdb = input("Would you like to search and download from a custom database?(y/n):").lower()

        if customdb == 'y':
            # Define the base URL
            base_url = input("Enter base url:")

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

        webdriver_service = Service(os.path.join(os.getcwd(), 'chromedriver-linux64', 'chromedriver'))

        driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
        wait = WebDriverWait(driver, 10)

        url = f"https://www.scienceopen.com/search#('v'~4_'id'~''_'queryType'~1_'context'~null_'kind'~77_'order'~0_'orderLowestFirst'~false_'query'~'{' '.join(search_terms)}'_'filters'~!('kind'~84_'openAccess'~true)*_'hideOthers'~false)"
        driver.get(url)

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
                break
            except StaleElementReferenceException:
                print("No more results to load.")
                break
            except Exception as other:
                print(f"An unknown exception occurred, please let the dev know: {other}")

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
                        EC.presence_of_element_located((By.CSS_SELECTOR, '#id2e > li:nth-child(1) > a:nth-child(1)')))
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

        driver.switch_to.window(driver.window_handles[0])

        driver.quit()
        pbar.close()

        print(f"Scraping completed. Successfully scraped {count} articles.")
        print(f"Failed to scrape {len(failed_articles)} articles.")
        print("Failed articles:")
        for article in failed_articles:
            print(article)

        return scraped_files

    def arxiv_search(search_terms, retmax, repository):
        query = "+AND+".join(search_terms).replace(' ', '%20')

        tracking_filename = os.path.join(os.getcwd(), 'search_info', 'arXiv', f"{'_'.join(search_terms)}_count.txt")
        fetched = 0
        if os.path.exists(tracking_filename):
            with open(tracking_filename, 'r') as f:
                fetched = int(f.read().strip())
        else:
            fetched = 0
            os.makedirs(os.path.join(os.getcwd(), 'search_info', 'arXiv'), exist_ok=True)

        MAX_RETRIES = 3
        SEARCH_MAX_RETRIES = 10
        scraped_files = []

        while fetched < retmax:
            current_max = min(10000, retmax - fetched)

            if repository == 'arxiv':
                api_url = f"https://export.arxiv.org/api/query?search_query=all:{query}&start={fetched}&max_results={current_max}"
            elif repository in ['biorxiv', 'medrxiv']:
                api_url = f"https://api.medrxiv.org/details/{repository}/all:{query}/0/json"
            elif repository == 'chemrxiv':
                api_url = f"https://chemrxiv.org/engage/chemrxiv/public-api/v1/items?term={query}&limit={current_max}"

            print(f"Fetching results from: {api_url}")

            search_retry_count = 0
            search_successful = False

            while search_retry_count < SEARCH_MAX_RETRIES and not search_successful:
                try:
                    print(f"Sending request to API... (Retry: {search_retry_count})")
                    response = requests.get(api_url)
                    if repository == 'arxiv':
                        xml_data = response.text
                        soup = BeautifulSoup(xml_data, "xml")
                        entries = soup.find_all("entry")
                    elif repository in ['biorxiv', 'medrxiv']:
                        json_data = response.json()
                        entries = json_data.get('papers', [])
                    elif repository == 'chemrxiv':
                        json_data = response.json()
                        entries = json_data.get('items', [])

                    if not entries:
                        print("No more results found. Exiting.")
                        break

                    print(f"Found {len(entries)} entries.")

                    for index, entry in enumerate(entries, start=1):
                        retry_count = 0
                        download_successful = False

                        while retry_count < MAX_RETRIES and not download_successful:
                            try:
                                print(f"Processing entry {index} out of {len(entries)}...")
                                # Your PDF download code here
                                # Save the downloaded file to the 'scraped_docs' directory
                                filename = f"{repository}_{fetched}.pdf"
                                with open(os.path.join(os.getcwd(), 'scraped_docs', filename), 'wb') as f:
                                    # Write the PDF content to the file
                                    # f.write(pdf_content)
                                    pass

                                scraped_files.append(filename)
                                fetched += 1
                                search_successful = True

                                with open(tracking_filename, 'w') as f:
                                    f.write(str(fetched))

                                print(f"Successfully processed entry {index}.")

                            except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                                retry_count += 1
                                print(
                                    f"Connection aborted for entry {index}. Retry attempt {retry_count}/{MAX_RETRIES}. Waiting for 10 seconds...")
                                time.sleep(10)

                        if not download_successful:
                            print(
                                f"Failed to download entry {index} after {MAX_RETRIES} attempts. Skipping this entry.")

                    search_successful = True

                except (requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
                    search_retry_count += 1
                    print(
                        f"Failed to fetch search results. Retry attempt {search_retry_count}/{SEARCH_MAX_RETRIES}. Error: {e}")
                    time.sleep(10)

            if not search_successful or not entries:
                break

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
            filename = f"unpaywall_{doi}.pdf"
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

        for chunk in query_chunks:
            if resume and chunk != last_chunk:
                continue
            query = " AND ".join(chunk)
            page = last_page if resume and chunk == last_chunk else 1

            while api_count < retmax:
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

                doi_list = [result['doi'] for result in unpaywall_data['results'] if 'doi' in result]

                if not doi_list:
                    print("No DOIs found in the Unpaywall API response.")
                    break

                downloaded_files = os.listdir(os.path.join(os.getcwd(), 'scraped_docs'))

                for doi in doi_list:
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
                            pdf_url = doi_data['best_oa_location']['url']
                            pdf_filename = download_pdf(pdf_url, doi)
                            if pdf_filename:
                                scraped_files.append(pdf_filename)

                        doi_data_str = json.dumps(doi_data, indent=4)
                        json_filename = f"unpaywall_{doi}.json"
                        with open(os.path.join(os.getcwd(), 'scraped_docs', json_filename), 'w') as f:
                            f.write(doi_data_str)
                        scraped_files.append(json_filename)

                        time.sleep(1 / 10)

                page += 1
                write_last_state(chunk, page)

        return scraped_files

    def remove_lines_after(line_number, file_path):
        lines = []
        with open(file_path, 'r') as file:
            for i, line in enumerate(file):
                if i < line_number:
                    lines.append(line)

        with open(file_path, 'w') as file:
            file.writelines(lines)

    output_directory_id, query_chunks = get_out_id(def_search_terms, maybe_search_terms)
    os.makedirs(os.path.join(os.getcwd(), 'scraped_docs'), exist_ok=True)
    os.makedirs(os.path.join(os.getcwd(), 'search_info'), exist_ok=True)

    search_info_file = os.path.join(os.getcwd(), 'search_info', f"{output_directory_id}.txt")

    if pubmedyn == "y":
        for chunk in query_chunks:
            print("Current search: " + str(chunk))
            scraped_files = pubmed_search(chunk, retmax)
            with open(search_info_file, 'a') as f:
                f.write('\n'.join(scraped_files) + '\n')

    if arxivyn == "y":
        for repository in repositories:
            for chunk in query_chunks:
                print("Current search: " + str(chunk))
                scraped_files = arxiv_search(chunk, retmax, repository)
                with open(search_info_file, 'a') as f:
                    f.write('\n'.join(scraped_files) + '\n')

    if soyn == "y":
        for chunk in query_chunks:
            print(f"Now running ScienceOpen scrape for {chunk}")
            scraped_files = scrape_scienceopen(chunk, retmax)
            with open(search_info_file, 'a') as f:
                f.write('\n'.join(scraped_files) + '\n')

    if upwyn == "y":
        scraped_files = unpaywall_search(query_chunks, retmax, email)
        with open(search_info_file, 'a') as f:
            f.write('\n'.join(scraped_files) + '\n')

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
                            pdf_response = requests.get(pdf_link)  # Removed 'https://' from here
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

    elif customdb == "n":
        None

    if auto is None:
        python = sys.executable
        os.execl(python, python, *sys.argv)

    else:
        return None
