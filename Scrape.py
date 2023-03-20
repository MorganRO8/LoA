def Scrape():
    import paperscraper
    import os
    from paperscraper.pdf import save_pdf_from_dump


    # update/download Metadata
    updownyn = input("Update/Download Metadata? (y/n)").lower()

    if updownyn == "y":
        from paperscraper.get_dumps import biorxiv, medrxiv, chemrxiv
        medrxiv()  # Takes ~30min and should result in ~35 MB file
        biorxiv()  # Takes ~1h and should result in ~350 MB file
        chemrxiv()  # Takes ~45min and should result in ~20 MB file

    elif updownyn == "n":
        None

    else:
        print("You must choose Y or N")

    # get search terms from user
    search_terms = input("Enter search terms (comma separated): ")
    search_terms = [term.strip() for term in search_terms.split(",")]

    # define queries using custom search terms
    queries = [query for query in search_terms if query]

    paperscraper.dump_queries([queries], '.')

    scholarbool = input("Would you like to search Google Scholar?(y/n):  ")

    if scholarbool == "y":
        from paperscraper.scholar import get_and_dump_scholar_papers

        scholar_query = input("Enter search query, as if google searching:  ")
        try:
            os.mkdir(os.getcwd() + "/scholar/")
        except:
            None

        try:
            get_and_dump_scholar_papers(scholar_query, str(os.getcwd()) + "/scholar/")
        except:
            print("Scholar search ended with error")

    else:
        None



    # Save PDFs in current folder and name the files by their DOI
    json_name = '_'.join(queries).replace(" ", "").lower()
    json_name = json_name

    try:
        os.mkdir(str(os.getcwd()) + "/pdfs")

    except:
        None

    try:
        os.mkdir(str(os.getcwd()) + '/pdfs/' + json_name + '/')

    except:
        None

    regdlyn = input("Would you like to try scrape pdfs that are available for free normally?(y/n)").lower()

    if regdlyn == "y":

        save_pdf_from_dump(str(os.getcwd()) + "/arxiv/" + json_name + '.jsonl', pdf_path=str(os.getcwd()) + '/pdfs/' + json_name + '/', key_to_save='doi')

        try:
            save_pdf_from_dump(str(os.getcwd()) + "/biorxiv/" + json_name + '.jsonl', pdf_path=str(os.getcwd()) + '/pdfs/' + json_name + '/', key_to_save='doi')
        except:
            print("biorxiv pdf save failed")

        try:
            save_pdf_from_dump(str(os.getcwd()) + "/chemrxiv/" + json_name + '.jsonl', pdf_path=str(os.getcwd()) + '/pdfs/' + json_name + '/', key_to_save='doi')
        except:
            print("chemrxiv pdf save failed")

        try:
            save_pdf_from_dump(str(os.getcwd()) + "/medrxiv/" + json_name + '.jsonl', pdf_path=str(os.getcwd()) + '/pdfs/' + json_name + '/', key_to_save='doi')
        except:
            print("medrxiv pdf save failed")

        try:
            save_pdf_from_dump(str(os.getcwd()) + "/pubmed/" + json_name + '.jsonl', pdf_path=str(os.getcwd()) + '/pdfs/' + json_name + '/', key_to_save='doi')
        except:
            print("pubmed pdf save failed")

    elif regdlyn == "n":
        None

    else:
        print("You must select y or n")

    scihubyn = input("Would you like to search and download from scihub?(y/n)")

    if scihubyn == "y":
        import re

        try:
            os.mkdir(str(os.getcwd()) + '/doi/')
        except:
            None

        try:
            # Open the file
            with open(str(os.getcwd()) + "/arxiv/" + json_name + '.jsonl', 'r') as f:
                # Read the file
                file_contents = f.read()

            # Search through the text file for any string that comes after "doi": " and before "}
            doi_list = re.findall(r'"doi": "(.*?)"', file_contents)

            # create a new text file named output that has each found instance of a string matching the criteria on it's own line
            with open(str(os.getcwd()) + '/doi/' + json_name + '_all_doi.txt', 'w') as f:
                for doi in doi_list:
                    f.write(doi + '\n')

        except:
            None

        try:
            # Open the file
            with open(str(os.getcwd()) + "/biorxiv/" + json_name + '.jsonl', 'r') as f:
                # Read the file
                file_contents = f.read()

            # Search through the text file for any string that comes after "doi": " and before "}
            doi_list = re.findall(r'"doi": "(.*?)"', file_contents)

            # create a new text file named output that has each found instance of a string matching the criteria on it's own line
            with open(str(os.getcwd()) + '/doi/' + json_name + '_all_doi.txt', 'w') as f:
                for doi in doi_list:
                    f.write(doi + '\n')

        except:
            None

        try:
            # Open the file
            with open(str(os.getcwd()) + "/chemrxiv/" + json_name + '.jsonl', 'r') as f:
                # Read the file
                file_contents = f.read()

            # Search through the text file for any string that comes after "doi": " and before "}
            doi_list = re.findall(r'"doi": "(.*?)"', file_contents)

            # create a new text file named output that has each found instance of a string matching the criteria on it's own line
            with open(str(os.getcwd()) + '/doi/' + json_name + '_all_doi.txt', 'w') as f:
                for doi in doi_list:
                    f.write(doi + '\n')

        except:
            None

        try:
            # Open the file
            with open(str(os.getcwd()) + "/medrxiv/" + json_name + '.jsonl', 'r') as f:
                # Read the file
                file_contents = f.read()

            # Search through the text file for any string that comes after "doi": " and before "}
            doi_list = re.findall(r'"doi": "(.*?)"', file_contents)

            # create a new text file named output that has each found instance of a string matching the criteria on it's own line
            with open(str(os.getcwd()) + '/doi/' + json_name + '_all_doi.txt', 'w') as f:
                for doi in doi_list:
                    f.write(doi + '\n')

        except:
            None

        try:
            # Open the file
            with open(str(os.getcwd()) + "/pubmed/" + json_name + '.jsonl', 'r') as f:
                # Read the file
                file_contents = f.read()

            # Search through the text file for any string that comes after "doi": " and before "}
            doi_list = re.findall(r'"doi": "(.*?)"', file_contents)

            # create a new text file named output that has each found instance of a string matching the criteria on it's own line
            with open(str(os.getcwd()) + '/doi/' + json_name + '_all_doi.txt', 'w') as f:
                for doi in doi_list:
                    f.write(doi + '\n')

            import tkinter as tk
            from scidownl import scihub_download

            # Go through each DOI number in the provided file and download the pdf from the website https://sci-hub.se using the package scidownl
            with open(str(os.getcwd()) + '/doi/' + json_name + '_all_doi.txt') as f:
                for line in f:
                    doi = line.strip()
                    paper_type = "doi"
                    paper = ("https://doi.org/" + doi)
                    try:
                        scihub_download(paper, paper_type=paper_type, out=str(os.getcwd() + '/pdfs/'))
                    except:
                        print("Error downloading " + doi)


        except:
            None

    elif scihubyn == "n":
        None

    else:
        print("You must choose y or n")


    import subprocess

    # fix pdf files that have html encoding
    # Go through each pdf in folder
    for filename in os.listdir(str(os.getcwd()) + '/pdfs/' + json_name + '/'):
        # Check if file is a pdf
        if filename.endswith(".pdf"):
            # Repair pdf using ghostscript
            try:
                subprocess.call(["gs", "-sDEVICE=pdfwrite", "-dPDFSETTINGS=/prepress", "-dNOPAUSE", "-dBATCH", "-sOutputFile=" + str(os.getcwd()) + '/pdfs/' + json_name + '/' + filename + ".repaired.pdf", str(os.getcwd()) + '/pdfs/' + json_name + '/' + filename])
            except:
                None
            # Delete original pdf
            try:
                os.remove(str(os.getcwd()) + '/pdfs/' + json_name + '/' + filename)
            except:
                None
            try:
                # Rename repaired pdf to original name
                os.rename(str(os.getcwd()) + '/pdfs/' + json_name + '/' + filename + ".repaired.pdf", str(os.getcwd()) + '/pdfs/' + json_name + '/' + filename)
            except:
                None


    import PyPDF2
    from os import listdir
    from os.path import isfile, join

    # Create list of all pdfs in input directory
    pdf_list = [f for f in listdir(str(os.getcwd()) + '/pdfs/' + json_name + '/') if isfile(join(str(os.getcwd()) + '/pdfs/' + json_name + '/', f))]

    # Create output text file
    try:
        os.mkdir(str(os.getcwd()) + "/txts/")
    except:
        None

    try:
        os.mkdir(str(os.getcwd()) + "/txts/" + json_name + "/")
    except:
        None

    output_file = open(str(os.getcwd()) + '/txts/' + json_name + "/output.txt", "w")

    for pdf in pdf_list:
        # Open PDF file
        pdf_file = open(str(os.getcwd()) + '/pdfs/' + json_name + '/' + pdf, "rb")
        # Create PyPDF2 object
        pdf_reader = PyPDF2.PdfReader(pdf_file)

        # Get number of pages in pdf
        num_pages = len(pdf_reader.pages)

        # For each page in the pdf, extract the text and write it to the output file
        for page_num in range(num_pages):
            page_obj = pdf_reader.pages[page_num]
            try:
                text = page_obj.extract_text().replace("\n", " ")
                output_file.write(text)
            except UnicodeDecodeError:
                print(f"Error: Could not extract text from page {page_num + 1} of {pdf}.")
                continue

        # Add a line break to separate each pdf
        output_file.write("\n")

    # Close output file
    output_file.close()

    with open(str(os.getcwd()) + "/txts/" + json_name + "/output.txt", 'r') as file:
        lines = file.readlines()

    with open(str(os.getcwd()) + "/txts/" + json_name + "/output.txt", 'w') as file:
        for line in lines:
            if line.strip():
                file.write(line)

    print("Text document containing all of the pdfs has been created, you may now run inference.")

    import sys
    python = sys.executable
    os.execl(python, python, *sys.argv)

