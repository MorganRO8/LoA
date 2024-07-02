import sqlite3
import sys
from src.utils import *

# Constants
CONVERT_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={}&format=json"
EUTILS_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={}"
ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
repositories = ['arxiv', 'chemrxiv']  # Decided to remove bio and med, as their api's are not very good


# I could be convinced to add them back, but because the api doesn't allow for search terms, I would need to write code
# to build a local database and search that, which would be time-consuming and a hassle for the end user.

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