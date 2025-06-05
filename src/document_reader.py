# Import necessary libraries and modules
from unstructured.staging.base import convert_to_dict
from unstructured.partition.auto import partition
from unstructured.partition.pdf import partition_pdf
from pdf2image.exceptions import PDFSyntaxError
import logging
import os
from src.utils import has_multiple_columns, xml_to_string, elements_to_string
from src.utils import print  # Custom print function for logging

# Configure logging to minimize output from the 'unstructured' library
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.CRITICAL)


from xml.etree import ElementTree as ET
import requests


def extract_images_from_pdf(pdf_path, output_dir):
    """Extract images from a PDF using pdfminer if available."""
    try:
        from pdfminer.high_level import extract_pages
        from pdfminer.layout import LTImage
    except Exception:
        print("pdfminer.six not installed; skipping image extraction")
        return

    os.makedirs(output_dir, exist_ok=True)
    img_count = 0
    try:
        for page_layout in extract_pages(pdf_path):
            for element in page_layout:
                if isinstance(element, LTImage) and hasattr(element, "stream"):
                    img_count += 1
                    img_data = element.stream.get_data()
                    img_path = os.path.join(output_dir, f"image_{img_count}.bin")
                    with open(img_path, "wb") as img_file:
                        img_file.write(img_data)
    except Exception as err:
        print(f"Failed to extract images from {pdf_path} due to {err}")


def extract_images_from_pubmed_xml(xml_string, output_dir):
    """Download images referenced in a PubMed XML document."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        root = ET.fromstring(xml_string)
    except Exception as err:
        print(f"Unable to parse XML for image extraction: {err}")
        return

    ns = {"xlink": "http://www.w3.org/1999/xlink"}
    for graphic in root.findall(".//graphic", ns):
        href = graphic.attrib.get("{http://www.w3.org/1999/xlink}href")
        if not href:
            continue
        url = href
        try:
            r = requests.get(url)
            if r.status_code == 200:
                filename = os.path.basename(url)
                with open(os.path.join(output_dir, filename), "wb") as f:
                    f.write(r.content)
        except Exception as err:
            print(f"Failed to download image {url} due to {err}")


def doc_to_elements(file, use_hi_res=False, use_multimodal=False):
    """
    Convert a document file to a structured text format.

    Args:
    file (str): Path to the input file.
    use_hi_res (bool): Flag to use high-resolution processing for PDFs.

    Returns:
    str: Formatted text content of the document.
    """
    # Create a directory for processed documents if it doesn't exist
    processed_docs_dir = os.path.join(os.getcwd(), 'processed_docs')
    os.makedirs(processed_docs_dir, exist_ok=True)

    # Generate the path for the processed file
    processed_file = os.path.splitext(os.path.basename(file))[0] + '.txt'
    processed_file_path = os.path.join(processed_docs_dir, processed_file)

    # Base name used for image directory
    paper_id = os.path.splitext(os.path.basename(file))[0]
    images_dir = os.path.join(os.getcwd(), 'images', paper_id)

    # If the file has already been processed, load its content
    formatted_output = None
    if os.path.exists(processed_file_path):
        with open(processed_file_path, 'r') as f:
            formatted_output = f.read()

    if formatted_output is None:
        print(f"Now processing {file}")
        elements = None

        # Process PDF files
        if file.endswith('.pdf'):
            if not use_hi_res:
                try:
                    elements = partition_pdf(filename=file, strategy='auto')
                except (PDFSyntaxError, TypeError) as er:
                    print(f"Failed to process {file} due to '{er}'.")
                    return None
            else:
                if has_multiple_columns(file):
                    print("Multiple columns detected, running default strategy")
                    try:
                        elements = partition_pdf(filename=file, strategy='auto')
                    except (PDFSyntaxError, TypeError) as er:
                        print(f"Failed to process {file} due to '{er}'.")
                        return None
                else:
                    print("Multiple columns not detected, running advanced strategy")
                    try:
                        elements = partition_pdf(filename=file, strategy='hi_res', infer_table_structure=True)
                    except (PDFSyntaxError, TypeError) as er:
                        print(f"Failed to process {file} due to '{er}'.")
                        return None

        # Process XML files
        elif file.endswith('.xml'):
            with open(file, 'r') as f:
                xml_content = f.read()
            formatted_output = xml_to_string(xml_content)
            with open(processed_file_path, 'w') as f:
                f.write(formatted_output)
        else:
            f = open(file, 'rb')
            try:
                elements = partition(file)
            except Exception as e:
                print(f"Unstructured failed because of {e}")

        if elements is not None and not formatted_output:
            formatted_output = elements_to_string(convert_to_dict(elements))

        if formatted_output:
            with open(processed_file_path, 'w') as f:
                f.write(formatted_output)

    # If multimodal, extract images
    if use_multimodal:
        if file.endswith('.pdf'):
            extract_images_from_pdf(file, images_dir)
        elif file.endswith('.xml'):
            if formatted_output is None:
                with open(file, 'r') as f:
                    xml_content = f.read()
            else:
                with open(file, 'r') as f:
                    xml_content = f.read()
            extract_images_from_pubmed_xml(xml_content, images_dir)

    return formatted_output
