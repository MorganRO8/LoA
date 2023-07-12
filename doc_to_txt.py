import logging
import os

import numpy as np
from pdf2image import convert_from_path
from pdf2image.exceptions import PDFSyntaxError
from unstructured.partition.auto import partition
from unstructured.partition.pdf import partition_pdf

from utils import select_scrape_results, get_out_id


def doc_to_txt(args):
    auto = args.get('auto')
    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')

    if auto is None:
        pdf_files_dir = select_scrape_results()

    else:
        output_directory_id, trash = get_out_id(def_search_terms, maybe_search_terms)
        pdf_files_dir = str(os.getcwd()) + '/scraped_docs/' + output_directory_id

    def has_multiple_columns(pdf_path):
        # Convert the first page of the PDF to an image
        images = convert_from_path(pdf_path, dpi=200, first_page=1, last_page=1)
        image = images[0]

        # Convert the image to grayscale and binarize it
        image = image.convert('L')
        image = image.point(lambda x: 0 if x < 128 else 255, '1')

        # Convert the image to a NumPy array for easier processing
        image_array = np.array(image)

        # Get the dimensions of the image
        height, width = image_array.shape

        # Scan the middle of the image vertically
        for x in range(width // 2 - 5, width // 2 + 5):
            white_pixels = 0
            for y in range(height):
                if image_array[y, x] == 255:
                    white_pixels += 1
                    if white_pixels > 50:
                        return True
                else:
                    white_pixels = 0

        # If no tall line of white pixels was found, return False
        return False

    # Turn off the ridiculous amount of logging unstructured does
    LOGGER = logging.getLogger()
    LOGGER.setLevel(logging.CRITICAL)

    # Set the number of threads in a slurm env
    os.environ["OMP_NUM_THREADS"] = os.getenv('SLURM_CPUS_PER_TASK', '1')  # default to 1 if the variable is not set
    print(f"Number of threads set to {os.environ['OMP_NUM_THREADS']}")

    try:
        os.mkdir(str(os.getcwd()) + '/txts/')
    except:
        None

    try:
        os.mkdir(str(os.getcwd()) + '/txts/' + output_directory_id)
    except:
        None

    # Directory where the text files will be stored
    text_files_dir = str(os.getcwd()) + '/txts/' + output_directory_id

    # Get the list of PDF files and TXT files
    pdf_files = sorted([filename for filename in os.listdir(pdf_files_dir) if filename.endswith('.pdf')])
    non_pdf_files = sorted([filename for filename in os.listdir(pdf_files_dir) if not filename.endswith('.pdf')])
    txt_files = sorted([filename for filename in os.listdir(text_files_dir) if filename.endswith('.txt')])

    # If there are already some TXT files processed
    if txt_files:
        last_processed_file = txt_files[-1].replace('.txt', '.pdf')
        last_index = pdf_files.index(last_processed_file)
        pdf_files = pdf_files[last_index + 1:]  # Ignore already processed files

    # Convert each PDF to a text file
    for filename in pdf_files:
        pdf_file_path = os.path.join(pdf_files_dir, filename)
        text_file_path = os.path.join(text_files_dir, filename.replace('.pdf', '.txt'))
        print(f"Now working on {filename}")

        try:
            columns = has_multiple_columns(pdf_file_path)

            if columns is True:

                print("Multiple columns detected, running default strategy")

                try:
                    # Partition the PDF into elements
                    elements = partition_pdf(filename=pdf_file_path, strategy='auto')

                    # Check if elements are empty
                    if not elements or all(not str(element).strip() for element in elements):
                        print(f"Skipping {filename} as it does not contain any text.")
                        continue

                    # Write the elements to a text file
                    with open(text_file_path, 'w') as file:
                        for element in elements:
                            file.write(str(element) + '\n')
                except (PDFSyntaxError, TypeError) as er:
                    print(f"Failed to process {filename} due to '{er}'.")
                    continue

            elif columns is False:

                print("Multiple columns not detected, running advanced strategy")

                try:
                    # Partition the PDF into elements
                    elements = partition_pdf(filename=pdf_file_path, strategy='hi_res', infer_table_structure=True)

                    # Check if elements are empty
                    if not elements or all(not str(element).strip() for element in elements):
                        print(f"Skipping {filename} as it does not contain any text.")
                        continue

                    # Write the elements to a text file
                    with open(text_file_path, 'w') as file:
                        for element in elements:
                            file.write(str(element) + '\n')
                except (PDFSyntaxError, TypeError) as er:
                    print(f"Failed to process {filename} due to '{er}'.")
                    continue
        except Exception as dangit:
            print(f"Got an exception, {dangit}")

    for filename in non_pdf_files:
        non_pdf_file_path = os.path.join(pdf_files_dir, filename)
        text_file_path = os.path.join(text_files_dir, filename.replace('.pdf', '.txt'))
        print(f"Now working on {filename}")

        try:
            # Partition the file into elements
            elements = partition(filename=non_pdf_file_path)

            # Check if elements are empty
            if not elements or all(not str(element).strip() for element in elements):
                print(f"Skipping {filename} as it does not contain any text.")
                continue

            # Write the elements to a text file
            with open(text_file_path, 'w') as file:
                for element in elements:
                    file.write(str(element) + '\n')
        except Exception as eek:
            print(f"Error processing {filename}; {eek}")
            continue
