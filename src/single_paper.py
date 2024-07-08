from pathlib import Path
import subprocess
import sys
import os
from src.utils import (download_ollama, select_schema_file)
from src.scrape import (pubmed_search, arxiv_search, scrape_scienceopen, unpaywall_search)

# URLs for PubMed Central API
ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'


def scrape_and_extract_concurrent(args):
    """
    Concurrently scrape papers from multiple sources and extract information based on a given schema.

    Args:
    args (dict): A dictionary containing configuration parameters.

    Returns:
    None
    """
    # Extract parameters from args or use default values
    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')
    schema_file = args.get('schema_file')
    user_instructions = args.get('user_instructions')
    auto = args.get('auto')
    model_name_version = args.get('model_name_version')
    retmax = args.get('retmax', 100)  # Default to 100 if not specified
    email = args.get('email')  # For Unpaywall

    # If not in auto mode, prompt for user inputs
    if auto is None:
        def_search_terms = input(
            "Enter 'definitely contains' search terms (comma separated) or type 'None': ").lower().split(',')
        maybe_search_terms = input(
            "Enter 'maybe contains' search terms (comma separated) or type 'None': ").lower().split(',')
        retmax = int(input("Enter the maximum number of papers to process per source: "))
        schema_file = select_schema_file()
        model_name_version = input("Please enter the model name and version (e.g., 'mistral:7b-instruct-v0.2-q8_0'): ")
        user_instructions = input("Please briefly tell the model what information it is trying to extract: ")

        # Ask which sources to search
        pubmedyn = input("Search PubMed? (y/n): ").lower() == 'y'
        arxivyn = input("Search arXiv? (y/n): ").lower() == 'y'
        chemrxivyn = input("Search ChemRxiv? (y/n): ").lower() == 'y'
        scienceopenyn = input("Search ScienceOpen? (y/n): ").lower() == 'y'
        unpaywallyn = input("Search Unpaywall? (y/n): ").lower() == 'y'
        if unpaywallyn:
            email = input("Enter email for Unpaywall API: ")
    else:
        # In auto mode, use provided args or default to searching all sources
        pubmedyn = args.get('pubmedyn', True)
        arxivyn = args.get('arxivyn', True)
        chemrxivyn = args.get('chemrxivyn', True)
        scienceopenyn = args.get('scienceopenyn', True)
        unpaywallyn = args.get('unpaywallyn', True)
        schema_file = os.path.join(os.getcwd(), 'dataModels', schema_file)

    # Prepare search terms
    search_terms = [term for term in def_search_terms if term.lower() != 'none']
    search_terms.extend([term for term in maybe_search_terms if term.lower() != 'none'])

    # Set up output directory
    output_dir = os.path.join(os.getcwd(), 'results')
    os.makedirs(output_dir, exist_ok=True)

    # Check for ollama binary and download if not present
    if not os.path.isfile('ollama'):
        print("ollama binary not found. Downloading the latest release...")
        download_ollama()

    # Start ollama server
    subprocess.Popen(["./ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

    # Debug version of starting the server
    # subprocess.Popen(["./ollama", "serve"])

    # Split model name and version
    try:
        model_name, model_version = model_name_version.split(':')
    except ValueError:
        model_name = model_name_version
        model_version = 'latest'
        model_name_version = f"{model_name}:{model_version}"

    # Check if the model is available, download if not
    model_file = os.path.join(str(Path.home()), ".ollama", "models", "manifests", "registry.ollama.ai", "library",
                              model_name, model_version)
    if not os.path.exists(model_file):
        print(f"Model file {model_file} not found. Pulling the model...")
        try:
            subprocess.run(["./ollama", "pull", model_name_version], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Failed to pull the model: {e}")
            return

    # Perform searches and extractions
    if pubmedyn:
        print("Searching PubMed...")
        pubmed_search(search_terms, retmax, concurrent=True, schema_file=schema_file,
                      user_instructions=user_instructions, model_name_version=model_name_version)

    if arxivyn:
        print("Searching arXiv...")
        arxiv_search(search_terms, retmax, 'arxiv', concurrent=True, schema_file=schema_file,
                     user_instructions=user_instructions, model_name_version=model_name_version)

    if chemrxivyn:
        print("Searching ChemRxiv...")
        arxiv_search(search_terms, retmax, 'chemrxiv', concurrent=True, schema_file=schema_file,
                     user_instructions=user_instructions, model_name_version=model_name_version)

    if scienceopenyn:
        print("Searching ScienceOpen...")
        scrape_scienceopen(search_terms, retmax, concurrent=True, schema_file=schema_file,
                           user_instructions=user_instructions, model_name_version=model_name_version)

    if unpaywallyn:
        if email is None:
            print("Email is required for Unpaywall search. Skipping Unpaywall.")
        else:
            print("Searching Unpaywall...")
            unpaywall_search([search_terms], retmax, email, concurrent=True, schema_file=schema_file,
                             user_instructions=user_instructions, model_name_version=model_name_version)

    print("Concurrent scraping and extraction completed.")

    # If not in auto mode, restart the script
    if auto is None:
        python = sys.executable
        os.execl(python, python, *sys.argv)
