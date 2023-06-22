import os
import requests
import sys
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
from scrapy import Spider, Request
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor
from scrapy_splash import SplashRequest
import asyncio
from pyppeteer import launch

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
    customdb = args.get('customdb')
    auto = args.get('auto')
    retmax = args.get('retmax')
    base_url = args.get('base_url')

    async def scrape_scienceopen(search_terms, retmax, output_directory_id):
        # Launch the browser
        browser = await launch()
        page = await browser.newPage()

        print(f"Now searching ScienceOpen for {search_terms}")

        # Generate the starting URL
        url = f"https://www.scienceopen.com/search#('v'~4_'id'~''_'queryType'~1_'context'~null_'kind'~77_'order'~0_'orderLowestFirst'~false_'query'~'{' '.join(search_terms)}'_'filters'~!('kind'~84_'openAccess'~true)*_'hideOthers'~false)"
        print(f"Starting url for current search: {url}")

        # Visit the URL
        print("Visiting page...")
        await page.goto(url)
        print("Page visited.")

        # Initialize the counter
        count = 0

        # Loop until we reach the maximum number of papers
        while count < retmax:
            print(f"Finding paper {count+1}/{retmax}")
            # Extract the article links
            article_links = await page.querySelectorAllEval('a[href^="https://www.scienceopen.com/document/"]',
                                                            '(elements) => elements.map((element) => element.href)')
            print(f"Found these links on the page:\n{article_links}")

            # Visit each article page
            for link in article_links:
                print(f"Following link: {link}")
                if count >= retmax:
                    break

                # Visit the article page
                print("Awaiting article page response...")
                await page.goto(link)
                print("Article page responded, extracting PDF link...")

                # Extract the PDF download link
                pdf_link = await page.querySelectorEval('a[title="Download PDF"]',
                                                        '(element) => element.getAttribute("onclick").match(/\'(https:.+)\'/)[1]')
                print(f"PDF link extracted: {pdf_link}\nDownloading PDF...")

                if pdf_link:
                    # Download the PDF
                    pdf_response = await page.goto(pdf_link)
                    print("PDF file accessed, saving to local machine...")

                    # Save the PDF
                    filename = pdf_link.split("/")[-1] + ".pdf"
                    with open(os.path.join(output_directory_id, filename), 'wb') as f:
                        f.write(await pdf_response.buffer())
                    print("PDF saved.")

                    # Increment the counter
                    count += 1

            # Click the "Load more" button
            await page.click('.so--tall')

        # Close the browser
        await browser.close()

    class XRXivQueryWrapper(XRXivQuery):
        def __init__(self, dump_filepath, retmax, fields=["title", "doi", "authors", "abstract", "date", "journal"]):
            super().__init__(dump_filepath, fields)
            self.retmax = retmax

        def search_keywords(self, keywords, fields=None, output_filepath=None):
            df = super().search_keywords(keywords, fields, output_filepath)
            return df.head(self.retmax)

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
            time.sleep(1/3)
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
                            future = executor.submit(querier, chunk, file_path)
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

        if soyn != "y":
            soyn = input("Would you like to scrape ScienceOpen?(y/n):").lower()

        if soyn == "y":
            runner = CrawlerRunner()

            for chunk in query_chunks:

                asyncio.get_event_loop().run_until_complete(scrape_scienceopen(chunk, retmax, output_directory_id))

    if soyn == "y":

        d = runner.join()
        d.addBoth(lambda _: reactor.stop())

        # Only start the reactor if it's not already running
        if not reactor.running:
            reactor.run()

        elif soyn != "n":
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


                   
