from unstructured.staging.base import convert_to_dict
from unstructured.partition.auto import partition
from unstructured.partition.pdf import partition_pdf
from pdf2image.exceptions import PDFSyntaxError
from src.utils import *
import logging

# Turn off the ridiculous amount of logging unstructured does
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.CRITICAL)


def doc_to_elements(file, use_hi_res=False):
    processed_docs_dir = os.path.join(os.getcwd(), 'processed_docs')
    os.makedirs(processed_docs_dir, exist_ok=True)

    processed_file = os.path.splitext(os.path.basename(file))[0] + '.txt'
    processed_file_path = os.path.join(processed_docs_dir, processed_file)

    if os.path.exists(processed_file_path):
        with open(processed_file_path, 'r') as f:
            return f.read()

    print(f"Now processing {file}")
    elements = None

    if file.endswith('.pdf'):
        if use_hi_res == False:
            try:
                # Partition the PDF into elements
                elements = partition_pdf(filename=file, strategy='auto')

            except (PDFSyntaxError, TypeError) as er:
                print(f"Failed to process {filename} due to '{er}'.")
                return None

        else:
            if has_multiple_columns(file):

                print("Multiple columns detected, running default strategy")

                try:
                    # Partition the PDF into elements
                    elements = partition_pdf(filename=file, strategy='auto')

                except (PDFSyntaxError, TypeError) as er:
                    print(f"Failed to process {filename} due to '{er}'.")
                    return None

            else:

                print("Multiple columns not detected, running advanced strategy")

                try:
                    # Partition the PDF into elements
                    elements = partition_pdf(filename=file, strategy='hi_res', infer_table_structure=True)

                except (PDFSyntaxError, TypeError) as er:
                    print(f"Failed to process {filename} due to '{er}'.")
                    return None

    elif file.endswith('.xml'):
        with open(file, 'r') as f:
            xml_content = f.read()
        formatted_output = xml_to_string(xml_content)
        with open(processed_file_path, 'w') as f:
            f.write(formatted_output)
        return formatted_output

    else:
        f = open(file, 'rb')

        try:
            elements = partition(file)

        except Exception as something_stupid:
            print(f"Unstructred failed because of {something_stupid}")

    formatted_output = elements_to_string(convert_to_dict(elements))
    with open(processed_file_path, 'w') as f:
        f.write(formatted_output)

    return formatted_output
