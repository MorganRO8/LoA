from pathlib import Path
import subprocess
from src.document_reader import doc_to_elements
from src.utils import *

ESEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
EFETCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'


def scrape_and_extract_concurrent(args):
    search_terms = args.get('search_terms')
    schema_file = args.get('schema_file')
    user_instructions = args.get('user_instructions')
    auto = args.get('auto')
    model_name_version = args.get('model_name_version')
    retmax = args.get('retmax')

    # Check if the ollama binary exists in the current working directory
    if not os.path.isfile('ollama'):
        print("ollama binary not found. Downloading the latest release...")
        download_ollama()
    else:
        print("ollama binary already exists in the current directory.")

    subprocess.Popen(["./ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    # subprocess.Popen(["./ollama", "serve"])

    if auto is None:
        search_terms = input("Enter search terms (comma-separated): ").split(',')
        retmax = int(input("Enter the maximum number of papers to process: "))

        # Select the schema file
        schema_file = select_schema_file()

        # Provide extraction information
        model_name_version = input("Please enter the model name and version (e.g., 'mistral:7b-instruct-v0.2-q8_0'): ")
        user_instructions = input(
            "Please briefly tell the model what information it is trying to extract, in the format of a "
            "command/instructions:\n\n")
    else:
        # output_directory_id, trash = get_out_id(def_search_terms, maybe_search_terms)
        # del trash
        # search_info_file = os.path.join(os.getcwd(), 'search_info', f"{output_directory_id}.txt")
        schema_file = os.path.join(os.getcwd(), 'dataModels', schema_file)
    try:
        model_name, model_version = model_name_version.split(':')
    except ValueError:
        model_name = model_name_version
        model_version = 'latest'

    output_dir = os.path.join(os.getcwd(), 'results')
    os.makedirs(output_dir, exist_ok=True)

    model_file = os.path.join(str(Path.home()), ".ollama", "models", "manifests", "registry.ollama.ai", "library",
                              model_name, model_version)

    if not os.path.exists(model_file):
        print(f"Model file {model_file} not found. Pulling the model...")
        try:
            subprocess.run(["./ollama", "pull", model_name], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Failed to pull the model: {e}")
            return

    # Load schema file
    schema_data, key_columns = load_schema_file(schema_file)

    num_columns = len(schema_data)

    headers = [schema_data[column_number]['name'] for column_number in range(1, num_columns + 1)] + ['paper']

    # print(f"Schema Data: \n{schema_data}\n\n")
    prompt = generate_prompt(schema_data, user_instructions, key_columns)

    csv_file = os.path.join(os.getcwd(), 'results', f"{os.path.splitext(schema_file)[0].split('/')[-1]}.csv")

    processed_pmids, no_fulltext_pmids = get_processed_pmids(csv_file)

    query = " AND ".join(search_terms) + " AND free full text[filter]"

    esearch_params = {
        'db': 'pmc',
        'term': query,
        'retmode': 'json',
        'retmax': retmax
    }

    examples = generate_examples(schema_data)

    print("Now performing esearch...")
    try:
        esearch_response = requests.get(ESEARCH_URL, params=esearch_params)
        esearch_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return
    esearch_data = esearch_response.json()

    if 'esearchresult' in esearch_data:
        uid_list = esearch_data['esearchresult']['idlist']

        if not uid_list:
            print("No search results found.")
            return

        downloaded_files = os.listdir(os.path.join(os.getcwd(), 'scraped_docs'))
        downloaded_files = [file.replace("pubmed_", "").replace(".xml", "") for file in
                            downloaded_files if "pubmed_" in file]

        # Count only the downloaded files that are part of this search
        downloaded_from_current_search = [uid for uid in uid_list if uid in downloaded_files]
        num_downloaded = len(downloaded_from_current_search)

        print(f"{num_downloaded} files already downloaded for this search.")

        ollama_url = "http://localhost:11434"
        max_retries = 3

        for uid in uid_list:
            if uid in processed_pmids:
                print(f"UID {uid} already processed. Skipping.")
                continue
            if uid in no_fulltext_pmids:
                print(f"UID {uid} previously found to have no full text. Skipping.")
                continue
            if num_downloaded >= retmax:
                print("Reached maximum number of downloads for this search. Stopping.")
                break
            if uid not in downloaded_files:

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
                    file_path = os.path.join(os.getcwd(), 'scraped_docs', filename)
                    with open(file_path, 'w') as f:
                        f.write(xml_data)
                    num_downloaded += 1
                else:
                    print(f"Full text not available for UID {uid}. Skipping.")
                    no_fulltext_pmids.add(uid)
                    with open(os.path.join(os.getcwd(), 'search_info', 'no_fulltext.txt'), 'a') as f:
                        f.write(f"{uid}\n")
                    time.sleep(0.35)
                    continue

                # Extract data from the scraped paper
                paper_content = truncate_text(doc_to_elements(file_path))

                retry_count = 0
                success = False

                while retry_count < max_retries and not success:
                    try:

                        model_options = {
                            "num_ctx": 32768,
                            "num_predict": 2048,
                            "mirostat": 0,
                            "mirostat_tau": 0.5,
                            "mirostat_eta": 1,
                            "tfs_z": 1,
                            "top_p": 1,
                            "top_k": 5,
                            "temperature": (0.35 * retry_count),
                            "repeat_penalty": (1.1 + (0.1 * retry_count)),
                            "stop": ["|||"],
                        }

                        prompt_with_content = (f"{prompt}\n\n{paper_content}\n\nAgain, please make sure to respond "
                                               f"only in the specified format exactly as described, or you will cause"
                                               f" errors.\nResponse:")
                        data = {
                            "model": model_name,
                            "prompt": prompt_with_content,
                            "stream": False,
                            "options": model_options
                        }
                        response = requests.post(f"{ollama_url}/api/generate", json=data)
                        response.raise_for_status()
                        result = response.json()["response"]

                        print("Unparsed result:")
                        print(result)

                        if result == '|||':
                            print(f"Got signal from model that the information is not present in {filename}")
                            retry_count = max_retries
                            continue

                        parsed_result = parse_llm_response(result, num_columns)
                        if not parsed_result:
                            print("Parsed Result empty, trying again")
                            retry_count += 1
                            continue
                        else:
                            print("Parsed result:")
                            print(parsed_result)

                        row = 0
                        item = 0

                        for row in parsed_result:
                            for item in row:
                                try:
                                    if item.lower().replace(" ",
                                                            "") == 'null' or item == '' or item == '""' or item == "''":
                                        parsed_result[row][item] = 'null'
                                except:
                                    pass

                        validated_result = validate_result(parsed_result, schema_data, examples)

                        if validated_result:
                            print("Validated result:")
                            print(validated_result)

                            for key_column in key_columns:
                                if key_column is not None:
                                    key_values = set()
                                    filtered_result = []
                                    for row in validated_result:
                                        key_value = row[key_column - 1]
                                        if key_value not in key_values:
                                            key_values.add(key_value)
                                            filtered_result.append(row)
                                    validated_result = filtered_result

                            for row in validated_result:
                                row.append(uid)

                            write_to_csv(validated_result, headers, filename=csv_file)

                            success = True

                        else:
                            print("Result failed to validate, trying again.")
                            retry_count += 1

                    except ValueError as e:
                        print(f"Validation failed for {filename}: {str(e)}")
                        retry_count += 1
                        print(f"Retrying ({retry_count}/{max_retries})...")

                    except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
                        print(f"Error processing {filename}: {type(e).__name__} - {str(e)}")
                        retry_count += 1
                        print(f"Retrying ({retry_count}/{max_retries})...")

                if not success:
                    print(f"Failed to extract data from {filename} after {max_retries} retries.")
                else:
                    processed_pmids.add(uid)
