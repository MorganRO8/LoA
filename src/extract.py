import subprocess
import sys
from pathlib import Path
from src.utils import *
from src.document_reader import doc_to_elements


def batch_extract(args):
    '''
    My original extract function, now re-named to make more sense! This is the version of the extract function used in
    the UI mode and automatic mode. It is designed to work in tandem with the scraping system and filestructure. It has
    an advantage in that it saves us from having to load the model for each paper, which is nice. However, it needs to
    run a batch process, so you'll have to have downloaded papers already, and have a list of them in the format and
    location the code is expecting. This will all be there if you used the scraping function.
    '''
    # Check if the ollama binary exists in the current working directory
    if not os.path.isfile('ollama'):
        print("ollama binary not found. Downloading the latest release...")
        download_ollama()
    else:
        print("ollama binary already exists in the current directory.")

    subprocess.Popen(["./ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    # subprocess.Popen(["./ollama", "serve"])

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
        user_instructions = input(
            "Please briefly tell the model what information it is trying to extract, in the format of a "
            "command/instructions:\n\n")
    else:
        output_directory_id, trash = get_out_id(def_search_terms, maybe_search_terms)
        del trash
        schema_file = os.path.join(os.getcwd(), 'dataModels', schema_file)
        search_info_file = os.path.join(os.getcwd(), 'search_info', f"{output_directory_id}.txt")
    try:
        model_name, model_version = model_name_version.split(':')
    except ValueError:
        model_name = model_name_version
        model_version = 'latest'

    if search_info_file == 'All':
        files_to_process = list_files_in_directory(os.path.join(os.getcwd(), 'scraped_docs'))
    else:
        with open(search_info_file, 'r') as f:
            files_to_process = f.read().splitlines()
        files_to_process = [file for file in files_to_process if
                            os.path.isfile(os.path.join(os.getcwd(), 'scraped_docs', file))]

    csv_file = os.path.join(os.getcwd(), 'results', f"{os.path.splitext(schema_file)[0].split('/')[-1]}.csv")

    # Check if the CSV file exists
    if os.path.exists(csv_file):
        # Read the last column of the CSV file to get the list of already processed papers
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            processed_papers = [row[-1] for row in reader]
            processed_papers = processed_papers[1:]  # Exclude the header row
    else:
        processed_papers = []

    # Filter the files_to_process list to exclude already processed papers
    files_to_process = [file for file in files_to_process if
                        os.path.splitext(os.path.basename(file))[0] not in processed_papers]

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

    max_retries = 3
    ollama_url = "http://localhost:11434"

    schema_data, key_columns = load_schema_file(schema_file)

    num_columns = len(schema_data)

    headers = [schema_data[column_number]['name'] for column_number in range(1, num_columns + 1)] + ['paper']

    prompt = generate_prompt(schema_data, user_instructions, key_columns)

    # print(f"Prompt before adding individual paper info:\n{prompt}")

    print(f"Found {len(files_to_process)} files to process, starting!")

    examples = generate_examples(schema_data)

    for file in files_to_process:

        print(f"Now processing {file}")

        file_path = os.path.join(os.getcwd(), 'scraped_docs', file)
        processed_file = os.path.splitext(file)[0] + '.txt'
        processed_file_path = os.path.join(os.getcwd(), 'processed_docs', processed_file)

        if os.path.exists(processed_file_path):
            with open(processed_file_path, 'r') as f:
                paper_content = truncate_text(f.read())
        else:
            try:
                paper_content = truncate_text(doc_to_elements(file_path))
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

                prompt_with_content = (f"{prompt}\n\n{paper_content}\n\nAgain, please make sure to respond only in the "
                                       f"specified format exactly as described, or you will cause errors.\nResponse:")
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
                print("Unparsed result:")
                print(result)

                if response == '|||':
                    print(f"Got signal from model that the information is not present in {file}")
                    retry_count = max_retries

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
                            if item.lower().replace(" ", "") == 'null' or item == '' or item == '""' or item == "''":
                                parsed_result[row][item] = 'null'
                        except:
                            # Do nothing!
                            None

                validated_result = validate_result(parsed_result, schema_data, examples)

                if validated_result:

                    print("Validated result:")
                    print(validated_result)

                    paper_filename = os.path.splitext(os.path.basename(file))[0]

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
                        row.append(paper_filename)

                    write_to_csv(validated_result, headers, filename=csv_file)

                    success = True

                else:
                    print("Result failed to validate, trying again.")
                    retry_count += 1

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


def extract(file_path, schema_file, model_name_version, user_instructions):
    '''
    This extract function is designed to take a file path as one of its arguments, instead of the original method.
    I think this will help greatly in setting up parallelization. One issue with this method is that it needs to load
    the model into memory every time it processes a file, but this actually happens very fast, and isn't too much of a
    slowdown when compared to the speed we'll gain (I think!)
    '''

    # Load schema file
    schema_data, key_columns = load_schema_file(schema_file)
    num_columns = len(schema_data)
    headers = [schema_data[column_number]['name'] for column_number in range(1, num_columns + 1)] + ['paper']

    # Generate prompt
    prompt = generate_prompt(schema_data, user_instructions, key_columns)
    examples = generate_examples(schema_data)

    # Process file
    try:
        paper_content = truncate_text(doc_to_elements(file_path))
    except Exception as err:
        print(f"Unable to process {file_path} into plaintext due to {err}")
        return None

    # Extract data
    max_retries = 3
    retry_count = 0
    success = False
    ollama_url = "http://localhost:11434"

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

            prompt_with_content = f"{prompt}\n\n{paper_content}\n\nAgain, please make sure to respond only in the specified format exactly as described, or you will cause errors.\nResponse:"
            data = {
                "model": model_name_version,
                "prompt": prompt_with_content,
                "stream": False,
                "options": model_options
            }
            response = requests.post(f"{ollama_url}/api/generate", json=data)
            response.raise_for_status()
            result = response.json()["response"]

            if result == '|||':
                print(f"Got signal from model that the information is not present in {file_path}")
                return None

            parsed_result = parse_llm_response(result, num_columns)
            if not parsed_result:
                print("Parsed Result empty, trying again")
                retry_count += 1
                continue

            validated_result = validate_result(parsed_result, schema_data, examples)

            if validated_result:
                paper_filename = os.path.splitext(os.path.basename(file_path))[0]

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
                    row.append(paper_filename)

                success = True
                return validated_result
            else:
                print("Result failed to validate, trying again.")
                retry_count += 1

        except Exception as e:
            print(f"Error processing {file_path}: {type(e).__name__} - {str(e)}")
            retry_count += 1
            print(f"Retrying ({retry_count}/{max_retries})...")

    if not success:
        print(f"Failed to extract data from {file_path} after {max_retries} retries.")
        return None
