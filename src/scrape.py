import sqlite3
import sys
import os
import requests
from urllib.parse import quote
from bs4 import BeautifulSoup
from src.utils import get_out_id
from src.utils import print  # Custom print function for logging

# Constants
ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
repositories = ['arxiv', 'chemrxiv']  # Decided to remove bio and med, as their api's are not very good
# I could be convinced to add them back, but because the api doesn't allow for search terms, I would need to write code
# to build a local database and search that, which would be time-consuming and a hassle for the end user.

class ScrapeParams():
    def __init__(self,args):
        self.auto = args.get('auto')
        self.def_search_terms = args.get('def_search_terms')
        self.maybe_search_terms = args.get('maybe_search_terms')
        self.pubmedyn = args.get('pubmedyn')
        self.arxivyn = args.get('arxivyn')
        self.soyn = args.get('ScienceOpenyn')
        self.customdb = args.get('customdb')
        self.retmax = args.get('retmax')
        self.base_url = args.get('base_url')
        self.upwyn = args.get('Unpaywallyn')
        self.email = args.get('Email')
        if self.auto is None:
            # Get 'definitely contains' search terms from user
            self.def_search_terms = input(
                "Enter 'definitely contains' search terms (comma separated) or type 'None' to only use maybe search terms: ").lower().split(
                ',')

            # Get 'maybe contains' search terms from user
            self.maybe_search_terms = input(
                "Enter 'maybe contains' search terms (comma separated) or type 'None' to only use definite search terms: ").lower().split(
                ',')

            # Define maximum returned papers per search term
            self.retmax = int(input("Set the maximum number of papers to fetch per search:"))
            attempt_count = 0
            while self.retmax < 1:
                print("Please enter a positive integer.")
                self.retmax = int(input("Set the maximum number of papers to fetch per search:"))
                attempt_count+=1
                if attempt_count == 5:
                    print("Sorry you're having difficulty. Setting max fetch to 10.")
                    self.retmax = 10 ## Prevent user from being stuck in an eternal loop if they don't know what a positive integer is after five tries.

            ## PubMed
            self.pubmedyn = self.get_yn_response("Would you like to search PubMed?(y/n): ",attempts=5)

            ## ArXiv
            self.arxivyn = self.get_yn_response("Would you like to search through the ArXivs?(y/n): ",attempts=5)
            
            ## ScienceOpen
            self.soyn = self.get_yn_response("Would you like to scrape ScienceOpen?(y/n): ",attempts=5)

            ## Unpaywall
            self.upwyn = self.get_yn_response("Would you like to scrape Unpaywall?(y/n): ",attempts=5)
            if self.upwyn == 'y':
                self.email = input("Enter email for use with Unpaywall:").lower()

            ## Custom Database
            self.customdb = self.get_yn_response("Would you like to search and download from a custom database?(y/n): ",attempts=5)
            if self.customdb == 'y':
                self.base_url = input("Enter base url:")

    def get_yn_response(prompt,attempts=5):
        response = input(prompt).lower()
        attempt_count = 0
        while response not in ["y","n"]:
            if attempt_count >4:
                print("Sorry you're having difficulty.  Setting response to 'n' and continuing onward.")
                return "n"
            print("Please enter either 'y' or 'n'. ")
            attempt_count += 1
            response = input(prompt).lower()
        return response

def main_scrape_pubmed(scrape_params,query_chunks,search_info_file):
    if scrape_params.pubmedyn == "y":
        from src.databases.pubmed import pubmed_search
        for chunk in query_chunks:
            print("Current search: " + str(chunk))
            scraped_files = pubmed_search(chunk, scrape_params.retmax)
            with open(search_info_file, 'a') as f:
                f.write('\n'.join(scraped_files) + '\n')

def main_scrape_arxiv(scrape_params,query_chunks,search_info_file):
    if scrape_params.arxivyn == "y":
        from src.databases.arxiv import arxiv_search
        for repository in repositories:
            for chunk in query_chunks:
                print("Current search: " + str(chunk))
                scraped_files = arxiv_search(chunk, scrape_params.retmax, repository)
                with open(search_info_file, 'a') as f:
                    f.write('\n'.join(scraped_files) + '\n')

def main_scrape_science_open(scrape_params,query_chunks,search_info_file):
    if scrape_params.soyn == "y":
        from src.databases.science_open import scrape_scienceopen
        for chunk in query_chunks:
            print(f"Now running ScienceOpen scrape for {chunk}")
            scraped_files = scrape_scienceopen(chunk, scrape_params.retmax)
            with open(search_info_file, 'a') as f:
                f.write('\n'.join(scraped_files) + '\n')

def main_scrape_unpaywall(scrape_params,query_chunks,search_info_file):
    if scrape_params.upwyn == "y":
        from src.databases.unpaywall import unpaywall_search
        scraped_files = unpaywall_search(query_chunks, scrape_params.retmax, scrape_params.email)
        with open(search_info_file, 'a') as f:
            f.write('\n'.join(scraped_files) + '\n')

def main_scrape_custom_db(scrape_params,query_chunks,search_info_file):
    if scrape_params.customdb == "y":
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
            params = tuple([f'%{term}%' for term in chunk] + [scrape_params.retmax])

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
                url = scrape_params.base_url + quote(doi)
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

    scrape_params = ScrapeParams(args)

    # Generate output directory ID and query chunks
    output_directory_id, query_chunks = get_out_id(scrape_params.def_search_terms, scrape_params.maybe_search_terms)

    # Create necessary directories
    os.makedirs(os.path.join(os.getcwd(), 'scraped_docs'), exist_ok=True)
    os.makedirs(os.path.join(os.getcwd(), 'search_info'), exist_ok=True)

    # Define the search info file path
    search_info_file = os.path.join(os.getcwd(), 'search_info', f"{output_directory_id}.txt")

    # Scrape from PubMed if selected
    main_scrape_pubmed(scrape_params,query_chunks,search_info_file)

    # Scrape from arXiv and ChemRxiv if selected
    main_scrape_arxiv(scrape_params,query_chunks,search_info_file)

    # Scrape from ScienceOpen if selected
    main_scrape_science_open(scrape_params,query_chunks,search_info_file)

    # Scrape from Unpaywall if selected
    main_scrape_unpaywall(scrape_params,query_chunks,search_info_file)

    # Scrape from custom database if selected
    main_scrape_custom_db(scrape_params,query_chunks,search_info_file)

    # If not in automatic mode, restart the script
    if scrape_params.auto is None:
        python = sys.executable
        os.execl(python, python, *sys.argv)

    # If in automatic mode, return None
    else:
        return None
