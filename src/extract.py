import os
import subprocess
import sys
import requests
import json
import torch
from pathlib import Path
from src.utils import *
from src.document_reader import doc_to_elements

def extract(args):
    #subprocess.Popen(["./ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    subprocess.Popen(["./ollama", "serve"])

    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')
    schema_file = args.get('schema_file')
    user_instructions = args.get('user_instructions')
    auto = args.get('auto')
    model_name_version = args.get('model_name_version')

    if auto is None:
        search_info_file = select_search_info_file()
        schema_file = select_schema_file()
        model_name_version = input("Please enter the model name and version (e.g., 'mistral:7b-instruct-v0.2-q8_0'): ")
        user_instructions = input("Please briefly tell the model what information it is trying to extract, in the format of a command/instructions:\n\n")
    else:
        output_directory_id, trash = get_out_id(def_search_terms, maybe_search_terms)
        del trash
        schema_file = os.path.join(os.getcwd(), 'dataModels', schema_file)
        search_info_file = os.path.join(os.getcwd(), 'search_info', f"{output_directory_id}.txt")
    
    model_name, model_version = model_name_version.split(':')

    with open(search_info_file, 'r') as f:
        files_to_process = f.read().splitlines()
    files_to_process = [file for file in files_to_process if os.path.isfile(os.path.join(os.getcwd(), 'scraped_docs', file))]

    output_dir = os.path.join(os.getcwd(), 'results')
    os.makedirs(output_dir, exist_ok=True)

    model_file = os.path.join(str(Path.home()), ".ollama", "models", "manifests", "registry.ollama.ai", "library", model_name, model_version)

    if not os.path.exists(model_file):
        print(f"Model file {model_file} not found. Pulling the model...")
        try:
            subprocess.run(["./ollama", "pull", model_name], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Failed to pull the model: {e}")
            return

    max_retries = 3
    ollama_url = "http://localhost:11434"
    
    schema_data, key_columns = load_schema_file(schema_file)

    num_columns = len(schema_data)
    
    headers = []
    current_header = 0
    
    while current_header > num_columns:
        headers.append(schema_data[n+1]['name'])
        current_header += 1
    
    prompt = generate_prompt(schema_data, user_instructions, key_columns)
    
    csv_file = os.path.join(os.getcwd(), 'results', f"{os.path.splitext(schema_file)[0].split('/')[-1]}.csv")
    
    num_gpus = torch.cuda.device_count()
    use_gpu = num_gpus > 0
    
    model_options = {
        "num_ctx": 32768,
        "num_predict": 2048,
        "top_p": 0.6,
        "temperature": 0.7,
        "repeat_penalty": 1.3,
        "stop": ["|||"],
        "num_gpu": num_gpus,
        "main_gpu": 0,
        "low_vram": False,
        "f16_kv": True,
        "use_mmap": True,
        "use_mlock": False
    }

    for file in files_to_process:
        
        print(f"Now processing {file}")
        
        file_path = os.path.join(os.getcwd(), 'scraped_docs', file)
        processed_file = os.path.splitext(file)[0] + '.txt'
        processed_file_path = os.path.join(os.getcwd(), 'processed_docs', processed_file)

        if os.path.exists(processed_file_path):
            with open(processed_file_path, 'r') as f:
                paper_content = f.read()
        else:
            try:
                paper_content = doc_to_elements(file_path)
            except Exception as err:
                print(f"Unable to process {file} into plaintext due to {err}")
                continue
            
        """  
        print("Paper contents:")
        print(paper_content)
        """
        
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                prompt_with_content = f"{prompt}\nHere are the paper contents:\n{paper_content}\n\nAgain, please make sure to respond only in the specified format exactly as described, or you will cause errors.\nResponse: "
                data = {
                    "model": model_name,
                    "prompt": prompt_with_content,
                    "stream": False,
                    "options": model_options
                }
                response = requests.post(f"{ollama_url}/api/generate", json=data)
                response.raise_for_status()
                result = response.json()["response"]
                
                # Was for debug
                #print("Unparsed result:")
                #print(result)
                
                if response == '|||':
                    print(f"Got signal from model that the information is not present in {file}")
                    retry_count = max_retries
                
                parsed_result = parse_llm_response(result, num_columns)
                # Was for debug
                #print("Parsed result:")
                #print(parsed_result)
                
                row = 0
                item = 0
                
                for row in parsed_result:
                    for item in row:
                        try:
                            if item.lower() == 'null' or item == '' or item == ' ':
                                parsed_result[row][item] = 'null'
                        except:
                            # Do nothing!
                            None

                validated_result = validate_result(parsed_result, schema_data)

                if validated_result is not None:                

                    print("Validated result:")
                    print(validated_result)
                    
                    paper_filename = os.path.splitext(os.path.basename(file))[0]
                    
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
                        row.append(paper_filename)
                    headers.append('paper')
                   
                    write_to_csv(validated_result, headers, filename=csv_file)
                    success = True

            except ValueError as e:
                print(f"Validation failed for {file}: {str(e)}")
                retry_count += 1
                print(f"Retrying ({retry_count}/{max_retries})...")

            except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
                print(f"Error processing {file}: {type(e).__name__} - {str(e)}")
                retry_count += 1
                print(f"Retrying ({retry_count}/{max_retries})...")

        if not success:
            print(f"Failed to extract data from {file} after {max_retries} retries.")

    if auto is None:
        python = sys.executable
        os.execl(python, python, *sys.argv)