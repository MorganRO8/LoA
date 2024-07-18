import subprocess
import sys
from pathlib import Path
import os
import csv
import requests
import json
from src.document_reader import doc_to_elements
from src.utils import (load_schema_file, generate_prompt, generate_examples,
                       parse_llm_response, validate_result, write_to_csv,
                       truncate_text, select_search_info_file, select_schema_file,
                       get_out_id, list_files_in_directory, download_ollama)
from src.classes import JobSettings

ExtractionDefaults={"max_retries":3,
                    "ollama_url":"http://localhost:11434"
                    }

class ExtractParams():
    def __init__(self,args):
        self.auto = args.get('auto')
        self.def_search_terms = args.get('def_search_terms')
        self.maybe_search_terms = args.get('maybe_search_terms')
        self.schema_file = args.get('schema_file')
        self.user_instructions = args.get('user_instructions')
        self.model_name_version = args.get('model_name_version')
        if self.auto is None:
            self.schema_file = select_schema_file()
            self.model_name_version = input("Please enter the model name and version (e.g., 'mistral:7b-instruct-v0.2-q8_0'): ")
            self.user_instructions = input(
                "Please briefly tell the model what information it is trying to extract, in the format of a "
                "command/instructions:\n\n")
        else:
            # In auto mode, construct file paths based on search terms
            self.output_directory_id, _ = get_out_id(self.def_search_terms, self.maybe_search_terms)
            self.schema_file = os.path.join(os.getcwd(), 'dataModels', self.schema_file)
        
        # Split model name and version
        try:
            self.model_name, self.model_version = self.model_name_version.split(':')
        except ValueError:
            self.model_name = self.model_name_version
            self.model_version = 'latest'
        
        ## Set up extraction parameters
        self.schema_data, self.key_columns = load_schema_file(self.schema_file)
        self.num_columns = len(self.schema_data)
        self.headers = [self.schema_data[column_number]['name'] for column_number in range(1, self.num_columns + 1)] + ['paper']
        self.prompt = generate_prompt(self.schema_data, self.user_instructions, self.key_columns)
        self.examples = generate_examples(self.schema_data)
        self.data = PromptData(self.model_name_version)

       
class PromptData():
    def __init__(self,model_name_version):
        self.model = model_name_version
        self.stream = False
        self.options = {
                        "num_ctx": 32768,
                        "num_predict": 2048,
                        "mirostat": 0,
                        "mirostat_tau": 0.5,
                        "mirostat_eta": 1,
                        "tfs_z": 1,
                        "top_p": 1,
                        "top_k": 5,
                        "stop": ["|||"],
                        }
        self.prompt = ""

    def _refresh_paper_content(self,file,prompt):
        file_path = os.path.join(os.getcwd(), 'scraped_docs', file)
        processed_file_path = os.path.join(os.getcwd(), 'processed_docs', os.path.splitext(file)[0] + '.txt')

        # Load paper content
        if os.path.exists(processed_file_path):
            with open(processed_file_path, 'r') as f:
                paper_content = truncate_text(f.read())
        else:
            try:
                paper_content = truncate_text(doc_to_elements(file_path))
            except Exception as err:
                print(f"Unable to process {file} into plaintext due to {err}")
                return True
        self.prompt = f"{prompt}\n\n{paper_content}\n\nAgain, please make sure to respond only in the specified format exactly as described, or you will cause errors.\nResponse:"
        return False
    
    def _refresh_data(self,retry_count):
        self.options["temperature"] = (0.35 * retry_count)
        self.options["repeat_penalty"] = (1.1 + (0.1 * retry_count))

    def __dict__(self):
        return {"model": self.model,
                "stream": self.stream,
                "options":self.options,
                "prompt":self.prompt}


def begin_ollama_server():
    # Check for ollama binary and download if not present
    if not os.path.isfile('ollama'):
        print("ollama binary not found. Downloading the latest release...")
        download_ollama()
    else:
        print("ollama binary already exists in the current directory.")

    # Start ollama server
    subprocess.Popen(["./ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

def get_files_to_process(extract_params,csv_file):
    ## Get "search_info_file"
    if extract_params.auto is None:
        search_info_file = select_search_info_file()
    else:
        search_info_file = os.path.join(os.getcwd(), 'search_info', f"{extract_params.output_directory_id}.txt")

    ## Process "search_info_file" to get list of files to process
    if search_info_file == 'All':
        files_to_process = list_files_in_directory(os.path.join(os.getcwd(), 'scraped_docs'))
    else:
        if os.path.exists(search_info_file):
            with open(search_info_file, 'r') as f:
                files_to_process = f.read().splitlines()
            files_to_process =  [file for file in files_to_process if os.path.isfile(os.path.join(os.getcwd(), 'scraped_docs', file))]
        else:
            files_to_process = []

    ## Check for already processed papers
    if os.path.exists(csv_file):
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            processed_papers = [row[-1] for row in reader]
            processed_papers = processed_papers[1:]  # Exclude the header row
    else:
        processed_papers = []

    # Filter out already processed papers
    files_to_process = [file for file in files_to_process if
                        os.path.splitext(os.path.basename(file))[0] not in processed_papers]

def check_model_file(extract_params):
    model_file = os.path.join(str(Path.home()), ".ollama", "models", "manifests", "registry.ollama.ai", "library",extract_params.model_name, extract_params.model_version)
    if not os.path.exists(model_file):
        print(f"Model file {model_file} not found. Pulling the model...")
        try:
            subprocess.run(["./ollama", "pull", extract_params.model_name_version], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Failed to pull the model: {e}")
            return True
        return False

def batch_extract(args):
    '''
    Batch extraction function for processing multiple papers.

    This function is designed to work with the scraping system and file structure.
    It processes multiple papers in a batch, which is more efficient as it only
    needs to load the model once. However, it requires pre-downloaded papers and
    a specific file structure.

    Args:
    args (dict): A dictionary containing configuration parameters.

    Returns:
    None: Results are written to a CSV file.
    '''

    ## Check for ollama binary and download if not present
    begin_ollama_server()

    ## Extract arguments or use default values
    extract_params = ExtractParams(args)

    ## Set up CSV file for results
    csv_file = os.path.join(os.getcwd(), 'results',f"{extract_params.model_name}_{extract_params.model_version}_{os.path.splitext(extract_params.schema_file)[0].split('/')[-1]}.csv")

    ## Determine which files to process
    files_to_process = get_files_to_process(extract_params, csv_file)

    ## Ensure output directory exists
    os.makedirs(os.path.join(os.getcwd(), 'results'), exist_ok=True)

    ## Check if the model is available, download if not
    if check_model_file(extract_params):
        return

    print(f"Found {len(files_to_process)} files to process, starting!")

    # Process each file
    for file in files_to_process:
        print(f"Now processing {file}")
        if extract_params.data._refresh_paper_content(file): 
            continue

        retry_count = 0
        success = False

        # Attempt extraction with retries
        while retry_count < ExtractionDefaults['max_retries'] and not success:
            extract_params.data._refresh_data(retry_count)
            try:
                response = requests.post(f"{ExtractionDefaults['ollama_url']}/api/generate", json=dict(extract_params.data))
                response.raise_for_status()
                result = response.json()["response"]

                print("Unparsed result:")
                print(result)

                # Check if the model is trying to tell us there are no results
                if result.strip().lower().replace("'", "").replace('"',
                                                                   '') == 'no information found' or result.strip() == '':
                    print(f"Got signal from model that the information is not present")
                    result = ", ".join(["null" for n in range(extract_params.num_columns)])

                # Parse and validate the result
                parsed_result = parse_llm_response(result, extract_params.num_columns)
                if not parsed_result:
                    print("Parsed result empty, trying again")
                    retry_count += 1
                    continue

                print("Parsed result:")
                print(parsed_result)

                # Clean up 'null' values
                for row in parsed_result:
                    for item in row:
                        try:
                            if item.lower().replace(" ",
                                                    "") == 'null' or item == '' or item == '""' or item == "''" or item.strip().lower().replace(
                                    '"', '').replace("'", "") == 'no information found':
                                parsed_result[row][item] = 'null'
                        except:
                            pass

                validated_result = validate_result(parsed_result, extract_params.schema_data, extract_params.examples, extract_params.key_columns)

                if validated_result:
                    print("Validated result:")
                    print(validated_result)

                    paper_filename = os.path.splitext(os.path.basename(file))[0]

                    # Filter results based on key columns
                    for key_column in extract_params.key_columns:
                        if key_column is not None:
                            key_values = set()
                            filtered_result = []
                            for row in validated_result:
                                key_value = row[key_column - 1]
                                if key_value not in key_values:
                                    key_values.add(key_value)
                                    filtered_result.append(row)
                            validated_result = filtered_result

                    # Add paper filename to each row
                    for row in validated_result:
                        row.append(paper_filename)

                    # Write results to CSV
                    write_to_csv(validated_result, extract_params.headers, filename=csv_file)

                    success = True
                else:
                    print("Result failed to validate, trying again.")
                    retry_count += 1

            except ValueError as e:
                print(f"Validation failed for {file}: {str(e)}")
                retry_count += 1
                print(f"Retrying ({retry_count}/{ExtractionDefaults['max_retries']})...")

            except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
                print(f"Error processing {file}: {type(e).__name__} - {str(e)}")
                retry_count += 1
                print(f"Retrying ({retry_count}/{ExtractionDefaults['max_retries']})...")

        if not success:
            print(f"Failed to extract data from {file} after {ExtractionDefaults['max_retries']} retries.")

    # If not in auto mode, restart the script
    if extract_params.auto is None:
        python = sys.executable
        os.execl(python, python, *sys.argv)


def extract(file_path, schema_file, model_name_version, user_instructions):
    '''
    Extract function for processing a single paper and writing results to a CSV file.

    Args:
    file_path (str): Path to the file to be processed.
    schema_file (str): Path to the schema file.
    model_name_version (str): Name and version of the model to use.
    user_instructions (str): Instructions for the model on what to extract.

    Returns:
    list or None: Validated results if successful, None if extraction fails.
    '''
    args = {'auto':True,
            'def_search_terms':None,
            'maybe_search_terms':None,
            'schema_file':schema_file,
            'user_instructions':user_instructions,
            'model_name_version':model_name_version
}
    extract_params = ExtractParams(args)

    # Prepare prompt data
    extract_params.data._refresh_paper_content(file_path,generate_prompt(extract_params.schema_data, user_instructions, extract_params.key_columns))

    
    validated_result = single_file_extract(extract_params,file_path)
    if not validated_result:
        print(f"Failed to extract data from {file_path} after {ExtractionDefaults['max_retries']} retries.")
        return None
    else:
        return validated_result


def single_file_extract(extract_params: ExtractParams, file_path):
    retry_count = 0
    while retry_count < ExtractionDefaults["max_retries"]:
        extract_params.data._refresh_data(retry_count)
        try:
            response = requests.post(f"{ExtractionDefaults['ollama_url']}/api/generate", json=dict(extract_params.data))
            response.raise_for_status()
            result = response.json()["response"]
            print(f"Unparsed Result:\n{result}")

            # Check if the model is trying to tell us there are no results
            if result.strip().lower().replace("'", "").replace('"', '') == 'no information found' or result.strip() == '':
                print(f"Got signal from model that the information is not present")
                result = ", ".join(["null" for n in range(extract_params.num_columns)])

            # Parse and validate the result
            parsed_result = parse_llm_response(result, extract_params.num_columns)
            if not parsed_result:
                print("Parsed Result empty, trying again")
                retry_count += 1
                continue
            print(f"Parsed Result:\n{parsed_result}")

            # Clean up 'null' values
            for row in parsed_result:
                for item in row:
                    try:
                        if any([item.lower().replace(" ","") == 'null',
                                item in ['','""',"''"], 
                                item.strip().lower().replace('"', '').replace("'", "") == 'no information found']):
                            parsed_result[row][item] = 'null'
                    except:
                        pass

            validated_result = validate_result(parsed_result, extract_params.schema_data, extract_params.examples, extract_params.key_columns)
            print(f"Validated Result:\n{validated_result}")
            if not validated_result:
                print("Result failed to validate, trying again.")
                retry_count += 1
                continue

            if validated_result:
                # Filter results based on key columns
                for key_column in extract_params.key_columns:
                    if key_column is None:
                        continue
                    key_values = set()
                    filtered_result = []
                    for row in validated_result:
                        key_value = row[key_column - 1]
                        if key_value not in key_values:
                            key_values.add(key_value)
                            row.append(os.path.splitext(os.path.basename(file_path))[0]) ## paper_filename # Add paper filename to each row if validated
                            filtered_result.append(row)
                    validated_result = filtered_result

                # Write results to CSV
                write_to_csv(validated_result, extract_params.headers, filename=os.path.join(os.getcwd(), 'results', f"{extract_params.model_name}_{extract_params.model_version}_{os.path.splitext(extract_params.schema_file)[0].split('/')[-1]}.csv"))
                return validated_result

        except Exception as e:
            print(f"Error processing {file_path}: {type(e).__name__} - {str(e)}")
            retry_count += 1
            print(f"Retrying ({retry_count}/{ExtractionDefaults['max_retries']})...")
    return None