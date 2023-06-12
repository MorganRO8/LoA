import os
import requests
import xml.etree.ElementTree as ET
import sys
from paperscraper.pdf import save_pdf_from_dump
import time
import itertools
from paperscraper.arxiv import get_and_dump_arxiv_papers
from paperscraper.xrxiv.xrxiv_query import XRXivQuery
import glob
import pkg_resources
from paperscraper.get_dumps import biorxiv, medrxiv, chemrxiv
import concurrent.futures
from arxiv.arxiv import UnexpectedEmptyPageError
import sqlite3
from urllib.parse import quote
from bs4 import BeautifulSoup

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

    def pubmed_search(search_terms, retmax, output_filename):
        # Combine search terms with AND operator
        query = " AND ".join(search_terms)

        # Set up the URL and parameters for esearch
        esearch_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
        esearch_params = {
            'db': 'pubmed',
            'term': query,
            'retmode': 'json'
        }

        # Call esearch
        print("Now performing esearch...")
        esearch_response = requests.get(esearch_url, params=esearch_params)
        esearch_data = esearch_response.json()

        # Check for 'esearchresult' in the response
        if 'esearchresult' in esearch_data:
            # Extract UIDs (PMIDs)
            uid_list = esearch_data['esearchresult']['idlist'][:retmax]
            uid_list_str = ','.join(uid_list)

            # Check if there are any UIDs
            if not uid_list:
                print("No search results found.")
                return

            # Set up the URL and parameters for efetch
            efetch_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
            efetch_params = {
                'db': 'pubmed',
                'id': uid_list,
                'retmode': 'xml',
                'rettype': 'medline'
            }

            # Call efetch
            print("Now performing efetch...")
            efetch_response = requests.get(efetch_url, params=efetch_params)
            xml_data = efetch_response.text

            try:
                # Parse XML and extract DOIs
                root = ET.fromstring(xml_data)
                dois = []
                for article_id_list in root.iter('ArticleIdList'):
                    for article_id in article_id_list.findall('ArticleId'):
                        if article_id.get('IdType') == 'doi':
                            dois.append(article_id.text)

                # Define the output file path
                output_filepath = os.path.join("pubmed", output_filename)

                # Read existing DOIs from the output file into a set
                existing_dois = set()
                if os.path.exists(output_filepath):
                    with open(output_filepath, "r") as f:
                        existing_dois = set(line.strip() for line in f)

                # Save the DOIs to the output file if not already present
                unique_dois = 0
                with open(output_filepath, "a") as f:
                    for doi in dois:
                        if doi not in existing_dois:
                            f.write(f"{doi}\n")
                            print("Written to output file: " + doi)
                            unique_dois += 1

                print(f"Saved DOIs for {unique_dois} new papers to {output_filename}")

            except ET.ParseError as e:
                print("Error parsing XML:", e)

    def remove_lines_after(line_number, file_path):
        lines = []
        with open(file_path, 'r') as file:
            for i, line in enumerate(file):
                if i < line_number:
                    lines.append(line)

        with open(file_path, 'w') as file:
            file.writelines(lines)

    def timed_get_and_dump_arxiv_papers(search_term, output_filepath):
        try:
            get_and_dump_arxiv_papers(search_term, output_filepath=output_filepath)
        except UnexpectedEmptyPageError:
            print(f"Skipping search term {search_term} due to an empty page error.")

    def create_directory(path):
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as error:
            print(f"Directory {path} can not be created")

    if def_search_terms is None:
        # get 'definitely contains' search terms from user
        def_search_terms = input("Enter 'definitely contains' search terms (comma separated): ").lower()
        def_search_terms = [term.strip() for term in def_search_terms.split(",")]
        def_search_terms.sort()

    if maybe_search_terms is None:
        # get 'maybe contains' search terms from user
        maybe_search_terms = input("Enter 'maybe contains' search terms (comma separated): ").lower()
        maybe_search_terms = [term.strip() for term in maybe_search_terms.split(",")]
        maybe_search_terms.sort()

    output_directory_id = f"{'_'.join(['def'] + def_search_terms + ['maybe'] + maybe_search_terms).replace(' ', '')}"
    output_filename = f"{'_'.join(['def'] + def_search_terms + ['maybe'] + maybe_search_terms).replace(' ', '')}_dois.txt"

    # define queries as all the combinations of 'maybe contains' search terms
    combinations = list(itertools.chain.from_iterable(itertools.combinations(maybe_search_terms, r) for r in range(0, len(maybe_search_terms) + 1)))
    queries = [def_search_terms + list(comb) for comb in combinations]
    print(" Ok! your adjusted searches are: " + str(queries))
    print("That's " + str(len(queries)) + " total combinations")
    if len(queries) > 100:
        print("This could take a while...")

    query_chunks = [queries[i:i + 1] for i in range(0, len(queries), 1)]

    # define maximum returned papers per search term
    if retmax is None:
        retmax = int(input("Set the maximum number of papers to fetch per search:"))

    if pubmedyn is None:
        pubmedyn = input("Would you like to search pubmed?(y/n)").lower()
    if pubmedyn == "y":
        for chunk in query_chunks:
            print("Current search: " + str(chunk))
            for search_term in chunk:
                pubmed_search(search_term, retmax, output_filename)
                time.sleep(1/3)
    else:
        None

    if arxivyn is None:
        arxivyn = input("Would you like to search through the arxivs?(y/n)").lower()

    if arxivyn == "y":

        # update/download Metadata
        if updownyn is None:
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
        querierchem = XRXivQuery(most_recent_chemrxiv_file)

        medrxiv_files = glob.glob(os.path.join(directory, 'medrxiv_*.jsonl'))
        most_recent_medrxiv_file = max(medrxiv_files, key=os.path.getctime)
        queriermed = XRXivQuery(most_recent_medrxiv_file)

        biorxiv_files = glob.glob(os.path.join(directory, 'biorxiv_*.jsonl'))
        most_recent_biorxiv_file = max(biorxiv_files, key=os.path.getctime)
        querierbio = XRXivQuery(most_recent_biorxiv_file)

        directories = ['pdfs', 'arxiv', 'chemrxiv', 'medrxiv', 'biorxiv']

        for directory in directories:
            create_directory(os.path.join(os.getcwd(), directory, output_directory_id))

        # Define the dictionary
        queriers = {
            "arxiv": timed_get_and_dump_arxiv_papers,
            "chemrxiv": querierchem.search_keywords,
            "medrxiv": queriermed.search_keywords,
            "biorxiv": querierbio.search_keywords
        }

        for chunk in query_chunks:
            for search_term in chunk:
                print("Current search: " + str(search_term))

                term_id = f"{'_'.join(search_term).replace(' ', '')}"

                for directory, querier in queriers.items():
                    file_path = os.path.join(os.getcwd(), directory, output_directory_id, term_id + ".jsonl")

                    if not os.path.isfile(file_path):
                        print(f'trying {directory} for {search_term}')

                        if directory == "arxiv":
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                future = executor.submit(querier, search_term, file_path)
                                try:
                                    future.result(timeout=300)
                                except concurrent.futures.TimeoutError:
                                    print(f"Skipping {directory} for {search_term} due to timeout.")
                        else:
                            try:
                                querier(search_term, output_filepath=file_path)
                            except:
                                print(f"Error occurred while querying {directory} for {search_term}")
                    else:
                        print(f"file for {directory} search already exists, skipping")

                    remove_lines_after(retmax, file_path)

                    try:
                        save_pdf_from_dump(file_path, pdf_path=os.path.join(os.getcwd(), 'pdfs', output_directory_id),
                                           key_to_save='doi')
                    except:
                        print(f"{directory} results empty, moving on")

    if customdb is None:
        customdb = input("Would you like to search and download from a custom database?\nYou will need to place your "
                         "metadata.db file in the operating directory, and provide a base url to fetch pdfs using "
                         "dois.(y/n)").lower()

    if customdb == "y":
        # Create a connection to the SQLite database
        conn = sqlite3.connect(str(os.getcwd()) + '/customdb/metadata.db')
        c = conn.cursor()

        if base_url is None:
            # Define the base URL
            base_url = input("Enter base url:")

        # Create the directory for the PDFs if it doesn't exist
        pdf_dir = os.path.join(os.getcwd(), 'customdb-pdf')
        os.makedirs(pdf_dir, exist_ok=True)
        print(f"PDFs will be saved to: {pdf_dir}")

        # Iterate over all search terms
        for chunk in query_chunks:
            for search_terms in chunk:
                print(f"Current search: {search_terms}")

                # Create the SQL query
                query = 'SELECT * FROM metadata WHERE '
                query += ' AND '.join([f'title LIKE ?' for _ in search_terms])
                query += ' ORDER BY title LIMIT ?'

                # Create the parameters for the SQL query
                params = tuple([f'%{search_term}%' for search_term in search_terms] + [retmax])

                # Execute the SQL query
                c.execute(query, params)

                # Fetch all the results
                results = c.fetchall()
                print(f"Found {len(results)} results for search terms: {search_terms}")

                # Iterate over the results
                for result in results:
                    # Extract the DOI
                    doi = result[0]
                    print(f"Processing DOI: {doi}")

                    # Create the URL for the paper
                    url = base_url + quote(doi)
                    print(f"URL: {url}")

                    # Get the webpage content
                    response = requests.get(url)

                    # Check if the request was successful
                    if response.status_code == 200:
                        # Parse the HTML
                        soup = BeautifulSoup(response.text, 'html.parser')

                        # Find the PDF link
                        pdf_link = None
                        for button in soup.find_all('button'):
                            if '.pdf' in button.get('onclick', ''):
                                pdf_link = button['onclick'].split("'")[1].replace("'location.href='//", '')
                                break

                        if pdf_link is not None:
                            # Download the PDF
                            pdf_response = requests.get('https://' + pdf_link)

                            if pdf_response.status_code == 200:
                                # Extract the PDF from the response
                                pdf = pdf_response.content

                                # Create the path for the PDF file
                                pdf_path = os.path.join(pdf_dir, f'{doi}.pdf')
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

