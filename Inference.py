def Inference(args):

    import os
    from unstructured.partition.pdf import partition_pdf
    from pdfminer.pdfparser import PDFSyntaxError
    from transformers import pipeline, AutoTokenizer

    questions = args.get('questions')
    selected_dir = args.get('selected_dir')
    model_id = args.get('model_id')
    auto = args.get('auto')

    def pdf_to_txt(selected_dir):
        # Directory where the PDFs are stored
        pdf_files_dir = str(os.getcwd()) + '/pdfs/' + selected_dir

        try:
            os.mkdir(str(os.getcwd()) + '/txts/')
        except:
            None

        try:
            os.mkdir(str(os.getcwd()) + '/txts/' + selected_dir)
        except:
            None

        # Directory where the text files will be stored
        text_files_dir = str(os.getcwd()) + '/txts/' + selected_dir

        # Get the list of PDF files and TXT files
        pdf_files = sorted([filename for filename in os.listdir(pdf_files_dir) if filename.endswith('.pdf')])
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
                # Partition the PDF into elements
                elements = partition_pdf(pdf_file_path)

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

    def get_questions():

        # create question
        questions = []

        while True:
            question = input("Please enter a question: ").lower()
            questions.append(question)

            another = input("Would you like to input another question? (y/n)").lower()
            if another.lower() == "n":
                break

        print("Here are the questions you entered:")
        for question in questions:
            print(question)

        return questions

    def select_scrape_results():
        import os

        # specify the directory path to list the subdirectories
        directory_path = str(os.getcwd()) + "/pdfs/"

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

    def get_and_write_answers(model_id, questions, selected_dir):
        import csv
        import textwrap

        # Directory where the text files are stored
        text_files_dir = str(os.getcwd()) + '/txts/' + selected_dir
        csv_file_path = str(os.getcwd()) + '/answers/' + selected_dir + '/' + '_'.join(questions).replace(' ', '').lower() + '.csv'

        # Get the list of TXT files
        txt_files = sorted([filename for filename in os.listdir(text_files_dir) if filename.endswith('.txt')])

        existing_dois = []

        # Check if CSV file exists
        if os.path.isfile(csv_file_path):
            with open(csv_file_path, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                existing_dois = [row[0] for row in reader]

        # Check for existing entries and skip them
        if existing_dois:
            last_processed_file = existing_dois[-1]
            last_index = txt_files.index(last_processed_file)
            txt_files = txt_files[last_index + 1:]  # Ignore already processed files

        # Open CSV file for writing
        with open(csv_file_path, 'a', newline='') as file:
            writer = csv.writer(file)

            # If CSV is empty, write headers
            if os.stat(csv_file_path).st_size == 0:
                headers = ["DOI"] + questions + ["References"]
                writer.writerow(headers)

            # Initialize the HuggingFace pipeline
            qa_pipeline = pipeline('question-answering', model=model_id)

            # Get the tokenizer to calculate the number of tokens
            tokenizer = AutoTokenizer.from_pretrained(model_id)

            # Query each question and write the answer to the CSV file
            for filename in txt_files:
                text_file_path = os.path.join(text_files_dir, filename)
                print(f"Getting answers from {filename} currently")

                with open(text_file_path, 'r') as file:
                    document = file.read()

                row = [filename]

                for question in questions:
                    print(question)
                    # Split the document into chunks that are smaller than the token limit
                    chunks = textwrap.wrap(document, width=tokenizer.model_max_length, break_long_words=False)

                    best_answer = None
                    best_score = -1

                    for chunk in chunks:
                        response = qa_pipeline({'context': chunk, 'question': question})

                        # Compare the confidence of the resulting answers
                        if response['score'] > best_score:
                            best_score = response['score']
                            best_answer = response['answer']

                    print(f"{filename} says the answer is: {best_answer}")
                    row.append(best_answer)

                writer.writerow(row)

    # have user select scrape results
    if selected_dir is None:
        selected_dir = select_scrape_results()

    if model_id is None:
        model_id = input("Please input the model id of the huggingface model you would like to use("
                         "ex:mrm8488/longformer-base-4096-finetuned-squadv2):")

    # make answers directory if necessary
    try:
        os.mkdir(str(os.getcwd()) + "/answers/")
    except:
        None

    try:
        os.mkdir(str(os.getcwd()) + "/answers/" + selected_dir)
    except:
        None

    # Display selection
    print("You selected:", selected_dir)

    # Get questions
    if questions is None:
        questions = get_questions()

    print("Now converting selected pdfs to plaintext...")
    pdf_to_txt(selected_dir)

    print("Now getting answers from papers (woop woop!)")
    get_and_write_answers(model_id, questions, selected_dir)

    print('Done!')

    if auto is None:
        import sys
        python = sys.executable
        os.execl(python, python, *sys.argv)

    else:
        return None
