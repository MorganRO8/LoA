import os
import requests
import sys
from tqdm import tqdm
from paperscraper.pdf import save_pdf_from_dump
import time
import itertools
from paperscraper.arxiv import *
from paperscraper.xrxiv.xrxiv_query import XRXivQuery
import glob
import pkg_resources
from paperscraper.get_dumps import biorxiv, medrxiv, chemrxiv
import concurrent.futures
import sqlite3
from urllib.parse import quote
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import StaleElementReferenceException
import logging
from selenium.webdriver.remote.remote_connection import LOGGER

# Constants
CONVERT_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={}&format=json"
EUTILS_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={}"
ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
DIRECTORIES = ['scraped_docs', 'arxiv', 'chemrxiv', 'medrxiv', 'biorxiv']


def Scrape(args):
    updownyn = args.get('updownyn')
    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')
    pubmedyn = args.get('pubmedyn')
    arxivyn = args.get('arxivyn')
    soyn = args.get('ScienceOpenyn')
    customdb = args.get('customdb')
    auto = args.get('auto')
    retmax = args.get('retmax')
    base_url = args.get('base_url')

    def scrape_scienceopen(search_terms, retmax, output_directory_id):
        # Turn off the ridiculous amount of logging selenium does
        LOGGER = logging.getLogger()
        LOGGER.setLevel(logging.WARNING)

        # Setup Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Ensure GUI is off
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

        # Set path to chromedriver as per your configuration
        webdriver_service = Service(ChromeDriverManager().install())

        # Choose Chrome Browser
        driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
        wait = WebDriverWait(driver, 10)

        url = f"https://www.scienceopen.com/search#('v'~4_'id'~''_'queryType'~1_'context'~null_'kind'~77_'order'~0_'orderLowestFirst'~false_'query'~'{' '.join(search_terms)}'_'filters'~!('kind'~84_'openAccess'~true)*_'hideOthers'~false)"
        driver.get(url)

        # Create a directory to store the scraped links if it doesn't exist
        scraped_links_dir = os.path.join(os.getcwd(), 'SO_searches')
        os.makedirs(scraped_links_dir, exist_ok=True)

        # Create a file to store the scraped links
        scraped_links_file_path = os.path.join(scraped_links_dir, f"{output_directory_id}.txt")

        # Load the already scraped links
        if os.path.exists(scraped_links_file_path):
            with open(scraped_links_file_path, 'r') as file:
                scraped_links = file.read().splitlines()
        else:
            scraped_links = []

        article_links = []
        while len(article_links) < retmax:
            # Extract all the article links
            new_links = driver.find_elements(By.CSS_SELECTOR, 'div.so-article-list-item > div > h3 > a')
            new_links = [link.get_attribute('href') for link in new_links if
                         link.get_attribute('href') not in scraped_links]

            # Filter out the already scraped links
            article_links.extend(new_links)

            # Click the "Load More" button
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

        start_time = time.time()
        pbar = tqdm(total=retmax, dynamic_ncols=True)
        count = 0
        for link in article_links:
            if count >= retmax:
                break

            # Open a new tab and switch to it
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[-1])

            try:
                # Visit the article page
                driver.get(link)

                # Extract the PDF download link
                pdf_link_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#id2e > li:nth-child(1) > a:nth-child(1)')))
                pdf_link = pdf_link_element.get_attribute('href')

                # Extract the DOI
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                doi_element = soup.find('meta', attrs={'name': 'citation_doi'})
                if doi_element is not None:
                    doi = doi_element.get('content')
                    encoded_doi = quote(doi, safe='')

                    # Download the PDF
                    pdf_response = requests.get(pdf_link)
                    filename = f"{encoded_doi}.pdf"
                    with open(os.path.join(os.getcwd(), 'scraped_docs', output_directory_id, filename), 'wb') as f:
                        f.write(pdf_response.content)

                    count += 1
                    elapsed_time = time.time() - start_time
                    avg_time_per_pdf = elapsed_time / count
                    est_time_remaining = avg_time_per_pdf * (retmax - count)
                    pbar.set_description(
                        f"DOI: {doi}, Count: {count}/{retmax}, Avg time per PDF: {avg_time_per_pdf:.2f}s, Est. time remaining: {est_time_remaining:.2f}s")
                    pbar.update(1)

                    # Append the scraped link to the file
                    with open(scraped_links_file_path, 'a') as file:
                        file.write(f"{link}\n")
            except Exception as e:
                print(f"An error occurred while processing the article: {e}")
            finally:
                # Close the tab and switch back to the original tab
                driver.close()
                driver.switch_to.window(driver.window_handles[0])

        driver.switch_to.window(driver.window_handles[0])

        driver.quit()
        pbar.close()

    class XRXivQueryWrapper(XRXivQuery):
        def __init__(self, dump_filepath, retmax, fields=["title", "doi", "authors", "abstract", "date", "journal"]):
            super().__init__(dump_filepath, fields)
            self.retmax = retmax

        def search_keywords(self, keywords, fields=None, output_filepath=None):
            df = super().search_keywords(keywords, fields, output_filepath)
            df = df.head(self.retmax)
            if output_filepath is not None:
                df.to_json(output_filepath, orient="records", lines=True)
            return df

    def get_and_dump_arxiv_papers_wrapper(keywords, output_filepath, retmax,
                                          fields=["title", "authors", "date", "abstract", "journal", "doi"], *args,
                                          **kwargs):
        papers = get_arxiv_papers(get_query_from_keywords(keywords), fields, max_results=retmax, *args, **kwargs)
        dump_papers(papers.head(retmax), output_filepath)

    def pubmed_search(search_terms, retmax, output_directory_id):
        # Combine search terms with AND operator
        query = " AND ".join(search_terms)

        # Set up the URL and parameters for esearch
        esearch_params = {
            'db': 'pubmed',
            'term': query,
            'retmode': 'json',
            'retmax': retmax
        }

        # Call esearch
        print("Now performing esearch...")
        try:
            esearch_response = requests.get(ESEARCH_URL, params=esearch_params)
            esearch_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return

        esearch_data = esearch_response.json()

        # Check for 'esearchresult' in the response
        if 'esearchresult' in esearch_data:
            # Extract UIDs (PMIDs)
            uid_list = esearch_data['esearchresult']['idlist']

            # Check if there are any UIDs
            if not uid_list:
                print("No search results found.")
                return

            # Get a list of already downloaded files
            downloaded_files = os.listdir(os.path.join(os.getcwd(), 'scraped_docs', output_directory_id))

            # For each UID, fetch the corresponding XML data
            for uid in uid_list:
                if uid not in downloaded_files:
                    # Set up the URL and parameters for efetch
                    efetch_params = {
                        'db': 'pubmed',
                        'id': uid,
                        'retmode': 'xml',
                        'rettype': 'full'
                    }

                    # Call efetch
                    print(f"Now performing efetch for UID {uid}...")
                    try:
                        efetch_response = requests.get(EFETCH_URL, params=efetch_params)
                        efetch_response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Request failed: {e}")
                        continue

                    xml_data = efetch_response.text

                    # Save the XML data to a file
                    with open(os.path.join(os.getcwd(), 'scraped_docs', output_directory_id, f"{uid}.xml"), 'w') as f:
                        f.write(xml_data)

                    # Sleep for 1/3 of a second to avoid hitting the rate limit
                    time.sleep(1/3)

    def remove_lines_after(line_number, file_path):
        lines = []
        with open(file_path, 'r') as file:
            for i, line in enumerate(file):
                if i < line_number:
                    lines.append(line)

        with open(file_path, 'w') as file:
            file.writelines(lines)

    def create_directory(path):
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as error:
            print(f"Directory {path} can not be created")

    if auto is None:
        # get 'definitely contains' search terms from user
        def_search_terms_input = input("Enter 'definitely contains' search terms (comma separated) or type 'None' to only use maybe search terms: ").lower()
        if def_search_terms_input == "none":
            print("No definite search terms selected.")
            def_search_terms = None
        else:
            def_search_terms = [term.strip() for term in def_search_terms_input.split(",")]
            def_search_terms.sort()

        # get 'maybe contains' search terms from user
        maybe_search_terms_input = input("Enter 'maybe contains' search terms (comma separated) or type 'None' to only use definite search terms: ").lower()
        if maybe_search_terms_input == "none":
            print("No maybe search terms selected, only using definite search terms.")
            maybe_search_terms = None
        else:
            maybe_search_terms = [term.strip() for term in maybe_search_terms_input.split(",")]
            maybe_search_terms.sort()

        # Check that at least one of def_search_terms or maybe_search_terms is not None
        if def_search_terms is None and maybe_search_terms is None:
            print("Error: Both definite and maybe search terms cannot be None.")
            return

    if maybe_search_terms is not None:
        if def_search_terms is not None:
            output_directory_id = f"{'_'.join(['def'] + def_search_terms + ['maybe'] + maybe_search_terms).replace(' ', '')}"

            # define queries as all the combinations of 'maybe contains' search terms
            combinations = list(itertools.chain.from_iterable(itertools.combinations(maybe_search_terms, r) for r in range(0, len(maybe_search_terms) + 1)))
            queries = [def_search_terms + list(comb) for comb in combinations]
        else:
            output_directory_id = f"{'_'.join(['maybe'] + maybe_search_terms).replace(' ', '')}"

            # define queries as all the combinations of 'maybe contains' search terms
            combinations = list(itertools.chain.from_iterable(itertools.combinations(maybe_search_terms, r) for r in range(1, len(maybe_search_terms) + 1)))
            queries = [list(comb) for comb in combinations]

        print(" Ok! your adjusted searches are: " + str(queries))
        print("That's " + str(len(queries)) + " total combinations")
        if len(queries) > 100:
            print("This could take a while...")

        query_chunks = queries

    else:
        output_directory_id = f"{'_'.join(['def'] + def_search_terms).replace(' ', '')}"

        query_chunks = [def_search_terms]

    for directory in DIRECTORIES:
        create_directory(os.path.join(os.getcwd(), directory, output_directory_id))
    create_directory(os.path.join(os.getcwd(), 'pubmed'))

    # define maximum returned papers per search term
    if auto is None:
        while True:
            try:
                retmax = int(input("Set the maximum number of papers to fetch per search:"))
                if retmax < 1:
                    print("Please enter a positive integer.")
                else:
                    break
            except ValueError:
                print("Please enter a valid number.")

    if auto is None:
        pubmedyn = input("Would you like to search pubmed?(y/n)").lower()
    if pubmedyn == "y":
        for chunk in query_chunks:
            print("Current search: " + str(chunk))
            pubmed_search(chunk, retmax, output_directory_id)
    else:
        None

    if auto is None:
        arxivyn = input("Would you like to search through the arxivs?(y/n)").lower()

    if arxivyn == "y":

        # update/download Metadata
        if auto is None:
            updownyn = input("Update/Download Metadata? (y/n)").lower()

        if updownyn == "y":
            medrxiv()  # Takes ~30min and should result in ~35 MB file
            biorxiv()  # Takes ~1h and should result in ~350 MB file
            chemrxiv()  # Takes ~45min and should result in ~20 MB file

        elif updownyn == "n":
            None

        else:
            print("You must choose y or n")

        directory = pkg_resources.resource_filename("paperscraper", "server_dumps")
        chemrxiv_files = glob.glob(os.path.join(directory, 'chemrxiv_*.jsonl'))
        most_recent_chemrxiv_file = max(chemrxiv_files, key=os.path.getctime)

        medrxiv_files = glob.glob(os.path.join(directory, 'medrxiv_*.jsonl'))
        most_recent_medrxiv_file = max(medrxiv_files, key=os.path.getctime)

        biorxiv_files = glob.glob(os.path.join(directory, 'biorxiv_*.jsonl'))
        most_recent_biorxiv_file = max(biorxiv_files, key=os.path.getctime)

        # Define the dictionary of queriers
        queriers = {
            "arxiv": get_and_dump_arxiv_papers_wrapper,
            "chemrxiv": XRXivQueryWrapper(most_recent_chemrxiv_file, retmax).search_keywords,
            "medrxiv": XRXivQueryWrapper(most_recent_medrxiv_file, retmax).search_keywords,
            "biorxiv": XRXivQueryWrapper(most_recent_biorxiv_file, retmax).search_keywords
        }

        for chunk in query_chunks:
            print("Current search: " + str(chunk))

            term_id = f"{'_'.join(chunk).replace(' ', '')}"

            for directory, querier in queriers.items():
                file_path = os.path.join(os.getcwd(), directory, output_directory_id, term_id + ".jsonl")

                if not os.path.isfile(file_path):
                    print(f'trying {directory} for {chunk}')

                    if directory == "arxiv":
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(querier, chunk, file_path, retmax)
                            try:
                                future.result(timeout=300)
                            except concurrent.futures.TimeoutError:
                                print(f"Skipping {directory} for {chunk} due to timeout.")
                                continue
                    else:
                        try:
                            querier(chunk, output_filepath=file_path)
                        except Exception as oops:
                            print(f"Error occurred while querying {directory} for {chunk} because {oops}")
                            continue
                else:
                    print(f"file for {directory} search already exists, skipping")
                    remove_lines_after(retmax, file_path)

                try:
                    save_pdf_from_dump(file_path, pdf_path=os.path.join(os.getcwd(), 'scraped_docs', output_directory_id),
                                       key_to_save='doi')
                except:
                    print(f"{directory} results empty, moving on")

    if auto is None:
        soyn = input("Would you like to scrape ScienceOpen?(y/n):").lower()

    while soyn != "n":

        if soyn is None:
            soyn = input("Would you like to scrape ScienceOpen?(y/n):").lower()

        if soyn == "y":
            for chunk in query_chunks:
                print(f"Now running ScienceOpen scrape for {chunk}")
                scrape_scienceopen(chunk, retmax, output_directory_id)
            break

        if soyn != "y" or "n":
            print("You must select y or n")

    if auto is None:
        customdb = input("Would you like to search and download from a custom database?\nYou will need to place your "
                         "metadata.db file in the operating directory, and provide a base url to fetch docs using "
                         "dois.(y/n)").lower()

    if customdb == "y":
        # Create a connection to the SQLite database
        conn = sqlite3.connect(str(os.getcwd()) + '/customdb/metadata.db')
        c = conn.cursor()

        if auto is None:
            # Define the base URL
            base_url = input("Enter base url:")

        # Create the directory for the docs if it doesn't exist
        pdf_dir = os.path.join(os.getcwd(), 'scraped_docs', output_directory_id)
        os.makedirs(pdf_dir, exist_ok=True)
        print(f"docs will be saved to: {pdf_dir}")

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
                            pdf_path = os.path.join(pdf_dir, f'{encoded_doi}.pdf')
                            print(f"Saving PDF to: {pdf_path}")


                            # Write the PDF to a file
                            with open(pdf_path, 'wb') as f:
                                f.write(pdf)
                        else:
                            print(f"Failed to download PDF. Status code: {pdf_response.status_code}")
                    else:
                        print("No PDF link found on the page. Saving the webpage as HTML.")

                        # Create the path for the HTML file
                        html_path = os.path.join(pdf_dir, f'{doi}.html')
                        print(f"Saving HTML to: {html_path}")

                        # Write the HTML to a file
                        with open(html_path, 'w') as f:
                            f.write(response.text)
                else:
                    print(f"Failed to access the webpage. Status code: {response.status_code}")

        # Close the connection to the database
        conn.close()

    elif customdb == "n":
        None

    if auto is None:
        python = sys.executable
        os.execl(python, python, *sys.argv)

    else:
        return None


                   
