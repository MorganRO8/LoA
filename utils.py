# Imports
import itertools
import os
import random
import shutil

from transformers import AutoTokenizer
from unstructured.documents.elements import NarrativeText
from unstructured.staging.huggingface import stage_for_transformers


def select_scrape_results():
    # specify the directory path to list the subdirectories
    directory_path = str(os.getcwd()) + "/scraped_docs/"

    # get a list of all directories in the specified directory
    subdirectories = [x for x in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, x))]

    # print out the list of directories
    print("The available scrape results are:")
    for i, dir in enumerate(subdirectories):
        print(f"{i + 1}. {dir}")

    # prompt the user to select a directory
    while True:
        selected_dir_num = input("Enter the number of the search you want to select: ")
        if selected_dir_num.isdigit() and 1 <= int(selected_dir_num) <= len(subdirectories):
            break
        print("Invalid input. Please enter a number between 1 and", len(subdirectories))

    # get the selected directory
    selected_dir = subdirectories[int(selected_dir_num) - 1]

    return selected_dir


def select_csv():
    # specify the directory path to list the subdirectories
    directory_path = os.path.join(os.getcwd(), 'snorkel')

    # get a list of all directories in the specified directory
    subdirectories = [x for x in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, x))]

    # print out the list of directories
    print("The available projects are:")
    for i, dir in enumerate(subdirectories):
        print(f"{i + 1}. {dir}")

    # prompt the user to select a directory
    while True:
        selected_dir_num = input("Enter the number of the project you want to select: ")
        if selected_dir_num.isdigit() and 1 <= int(selected_dir_num) <= len(subdirectories):
            break
        print("Invalid input. Please enter a number between 1 and", len(subdirectories))

    # get the selected directory
    selected_dir = subdirectories[int(selected_dir_num) - 1]

    return selected_dir


def prepare_test_folder(directory):
    # check if 'test' directory exists
    test_dir = os.path.join(directory, 'train')
    if not os.path.exists(test_dir):
        # get all .txt files in the directory
        txt_files = [f for f in os.listdir(directory) if f.endswith(".txt")]
        # create 'train' directory
        os.mkdir(test_dir)
        # calculate number of files to move (10% of all .txt files)
        num_files_to_move = len(txt_files) // 10
        # select random files to move
        files_to_move = random.sample(txt_files, num_files_to_move)
        # move selected files to 'test' directory
        for file in files_to_move:
            shutil.move(os.path.join(directory, file), os.path.join(test_dir, file))


def load_text_files(directory, model_type):
    texts = []

    # get tokenizer and model using model_type
    tokenizer = AutoTokenizer.from_pretrained(model_type)

    # ensure training subset directory is prepared before loading files
    prepare_test_folder(directory)

    for filename in os.listdir(os.path.join(directory, 'train')):
        if filename.endswith(".txt"):
            with open(os.path.join(directory, filename), 'r') as f:
                file_text = f.read()
                narrative = NarrativeText(text=file_text)
                elements = stage_for_transformers([narrative], tokenizer)
                for element in elements:
                    texts.append(element.text)
    return texts


def get_out_id(def_search_terms_input, maybe_search_terms_input):
    # Method for getting terms from user, to be moved to json generator for auto mode
    """

    """
    if def_search_terms_input == "none":
        print("No definite search terms selected.")
        def_search_terms = None
    else:
        def_search_terms = [term.strip() for term in def_search_terms_input.split(",")]
        def_search_terms.sort()
    # Method for getting terms from user, to be moved to json generator for auto mode
    """
    
    """
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
            combinations = list(itertools.chain.from_iterable(
                itertools.combinations(maybe_search_terms, r) for r in range(0, len(maybe_search_terms) + 1)))
            queries = [def_search_terms + list(comb) for comb in combinations]
        else:
            output_directory_id = f"{'_'.join(['maybe'] + maybe_search_terms).replace(' ', '')}"

            # define queries as all the combinations of 'maybe contains' search terms
            combinations = list(itertools.chain.from_iterable(
                itertools.combinations(maybe_search_terms, r) for r in range(1, len(maybe_search_terms) + 1)))
            queries = [list(comb) for comb in combinations]

        print(" Ok! your adjusted searches are: " + str(queries))
        print("That's " + str(len(queries)) + " total combinations")
        if len(queries) > 100:
            print("This could take a while...")

        query_chunks = queries

    else:
        output_directory_id = f"{'_'.join(['def'] + def_search_terms).replace(' ', '')}"

        query_chunks = [def_search_terms]

    return output_directory_id, query_chunks
