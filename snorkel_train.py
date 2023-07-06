# Imports
import os
import re
import spacy
import pandas as pd
import torch
from nltk.corpus import wordnet
from transformers import pipeline
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from snorkel.labeling import PandasLFApplier
from snorkel.labeling.model import LabelModel
from transformers import AutoModelForQuestionAnswering, AutoTokenizer, TrainingArguments, Trainer
from snorkel.types import DataPoint
from sklearn.model_selection import train_test_split
import optuna
from torch.utils.data import Subset
from functools import partial
from unstructured.partition.auto import partition
from unstructured.partition.pdf import partition_pdf
from pdfminer.pdfparser import PDFSyntaxError
import logging
from pdf2image import convert_from_path
from PIL import Image
import numpy as np


def snorkel_train(args):
    tasks = args.get('tasks')
    text_dir = args.get('text_directory')
    model_type = args.get('model_type')
    model_path = args.get('model_path')
    auto = args.get('auto')

    # Definitions
   

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

    
    def doc_to_txt(text_dir):
    
        # Turn off the ridiculous amount of logging unstructured does
        LOGGER = logging.getLogger()
        LOGGER.setLevel(logging.CRITICAL)
        
        # Set the number of threads in a slurm env
        os.environ["OMP_NUM_THREADS"] = os.getenv('SLURM_CPUS_PER_TASK', '1')  # default to 1 if the variable is not set
        print(f"Number of threads set to {os.environ['OMP_NUM_THREADS']}")
    
        # Directory where the PDFs are stored
        pdf_files_dir = str(os.getcwd()) + '/scraped_docs/' + text_dir

        try:
            os.mkdir(str(os.getcwd()) + '/txts/')
        except:
            None

        try:
            os.mkdir(str(os.getcwd()) + '/txts/' + text_dir)
        except:
            None

        # Directory where the text files will be stored
        text_files_dir = str(os.getcwd()) + '/txts/' + text_dir

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
                # Partition the PDF into elements
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

    
    def get_keywords(keywords):
        if keywords is None:
            user_input = input("Enter a comma-separated list of keywords or a path to a .txt file: ")
            if os.path.isfile(user_input):
                with open(user_input, 'r') as f:
                    keywords = f.read().splitlines()
            else:
                keywords = user_input.split(',')
        return keywords

    def get_model_identifiers(model_identifiers):
        if model_identifiers is None:
            user_input = input("Enter a comma-separated list of Hugging Face model identifiers: ")
            model_identifiers = user_input.split(',')
        return model_identifiers

    def get_questions(questions):
        if questions is None:
            user_input = input("Enter a comma-separated list of yes/no questions: ")
            questions = user_input.split(',')
        return questions

    def get_synonyms(word):
        synonyms = set()
        for syn in wordnet.synsets(word):
            for lemma in syn.lemmas():
                synonyms.add(lemma.name())
        return list(synonyms)

    def generate_synonym_sentences(sentence):
        doc = nlp(sentence)
        synonym_sentences = []
        for token in doc:
            if token.dep_ in ['nsubj', 'dobj']:
                synonyms = get_synonyms(token.text)
                for synonym in synonyms:
                    synonym_sentence = sentence.replace(token.text, synonym)
                    synonym_sentences.append(synonym_sentence)
        return synonym_sentences

    def escape_nouns(sentence):
        doc = nlp(sentence)
        for token in doc:
            if token.pos_ == 'NOUN':
                sentence = sentence.replace(token.text, r'\b\w+\b')
        return sentence

    def get_sentences(sentences):
        if sentences is None:
            user_input = input("Enter a comma-separated list of sentences or a path to a .txt file: ")
            if os.path.isfile(user_input):
                with open(user_input, 'r') as f:
                    sentences = [line.strip() for line in f]
            else:
                sentences = user_input.split(',')
        return sentences
    
    def select_scrape_results():
        import os

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

    def load_text_files(directory):
        texts = []
        for filename in os.listdir(directory):
            if filename.endswith(".txt"):
                with open(os.path.join(directory, filename), 'r') as f:
                    texts.append(f.read())
        return texts

    if auto is None:
        # Have user select scrape results to use
        text_dir = select_scrape_results()

    # Convert pdfs to plaintext
    print("Now converting selected pdfs to plaintext...")
    doc_to_txt(text_dir)

    # Initialization
    nlp = spacy.load('en_core_web_sm')
    if auto is None:
        tasks = []
        task_count = int(input("Enter the number of tasks: ")) if tasks is None else len(tasks)
        for i in range(task_count):
            task_name = input(f"Enter the name of task {i + 1}: ")
            keywords = get_keywords(None)
            model_identifiers = get_model_identifiers(None)
            questions = get_questions(None)
            sentences = get_sentences(None)
            tasks.append({
                'name': task_name,
                'keywords': keywords,
                'model_identifiers': model_identifiers,
                'questions': questions,
                'sentences': sentences,
            })
    else:
        new_tasks = []
        for task in tasks:
            task_name = task.get('name')
            keywords = get_keywords(task.get('keywords'))
            model_identifiers = get_model_identifiers(task.get('model_identifiers'))
            questions = get_questions(task.get('questions'))
            sentences = get_sentences(task.get('sentences'))
            new_tasks.append({
                'name': task_name,
                'keywords': keywords,
                'model_identifiers': model_identifiers,
                'questions': questions,
                'sentences': sentences,
            })
        tasks = new_tasks

    # Load text files
    texts = load_text_files(text_dir)

    # Convert texts to DataFrame
    df_train = pd.DataFrame(texts, columns=['text'])

    # Instantiate lfs list
    lfs = []

    # Define the labeling functions outside the loop
    def lf_keyword_search(x: DataPoint, keyword) -> int:
        return 1 if keyword in x.text.lower() else 0

    def lf_question_answering(x: DataPoint, nlp, question) -> int:
        answer = nlp(question=question, context=x.text)
        if answer['score'] > 0.5:
            return 1 if answer['answer'].lower() == 'yes' else -1
        else:
            return 0

    def lf_sentence_similarity(x: DataPoint, model, sentence_embedding) -> int:
        x_embedding = model.encode([x.text])
        similarity = cosine_similarity([sentence_embedding], [x_embedding])[0][0]
        if similarity > 0.8:
            return 1
        elif similarity > 0.3:
            return 0
        else:
            return -1

    def lf_sentence_matching(x: DataPoint, sentence) -> int:
        return 1 if re.search(sentence, x.text) else 0

    # Labeling Functions
    for task in tasks:
        for keyword in task['keywords']:
            lf = partial(lf_keyword_search, keyword=keyword)
            lfs.append(lf)

        for model_id in task['model_identifiers']:
            for question in task['questions']:
                nlpr = pipeline('question-answering', model=model_id)
                lf = partial(lf_question_answering, nlp=nlpr, question=question)
                lfs.append(lf)

        comprehensive_sentences = []
        for sentence in task['sentences']:
            escaped_sentence = escape_nouns(sentence)
            synonym_sentences = generate_synonym_sentences(escaped_sentence)
            comprehensive_sentences.extend(synonym_sentences)

        comprehensive_sentences = list(set(comprehensive_sentences))  # remove duplicates

        for model_id in task['model_identifiers']:
            model = SentenceTransformer(model_id)

            for sentence in comprehensive_sentences:
                sentence_embedding = model.encode([sentence])
                lf = partial(lf_sentence_similarity, model=model, sentence_embedding=sentence_embedding)
                lfs.append(lf)

                lf = partial(lf_sentence_matching, sentence=sentence)
                lfs.append(lf)

        # Apply the labeling functions to your dataset
        applier = PandasLFApplier(lfs)
        L_train = applier.apply(df_train)

        # Train a Snorkel LabelModel to combine the labels
        label_model = LabelModel(cardinality=2, verbose=True)
        label_model.fit(L_train, n_epochs=500, log_freq=100, seed=123)

        # Transform the labels into a single set of noise-aware probabilistic labels
        df_train[task['name'] + "_label"] = label_model.predict(L=L_train, tie_break_policy="abstain")

    # Save the DataFrame to a CSV file
    snorkel_dir = os.getcwd() + '/snorkel/'
    os.makedirs(snorkel_dir, exist_ok=True)  # create the directory if it doesn't exist
    df_train.to_csv(snorkel_dir + 'training_data.csv', index=False)

    # Load the DataFrame from the CSV file
    df_train = pd.read_csv(snorkel_dir + 'training_data.csv')

    # Define a PyTorch dataset
    class SnorkelDataset(torch.utils.data.Dataset):
        def __init__(self, encodings, labels):
            self.encodings = encodings
            self.labels = labels

        def __getitem__(self, idx):
            item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
            item['labels'] = torch.tensor(self.labels[idx])
            return item

        def __len__(self):
            return len(self.encodings.input_ids)

    # Prepare the dataset for training
    if auto is None:
        model_type = input("Enter the Hugging Face model type for the final model (e.g., 'bert-base-uncased'): ")
    tokenizer = AutoTokenizer.from_pretrained(model_type)
    train_encodings = tokenizer(df_train['text'].tolist(), truncation=True, padding=True)

    # Create a dataset and split into training and validation sets
    train_datasets = []
    val_datasets = []
    for task in tasks:
        labels = df_train[task['name'] + "_label"].tolist()
        dataset = SnorkelDataset(train_encodings, labels)

        # Split the dataset for this task into training and validation sets
        train_indices, val_indices = train_test_split(list(range(len(dataset))), test_size=0.2, random_state=42)

        train_dataset = Subset(dataset, train_indices)
        val_dataset = Subset(dataset, val_indices)

        train_datasets.append(train_dataset)
        val_datasets.append(val_dataset)

    # Concatenate all the training datasets and all the validation datasets
    train_dataset = torch.utils.data.ConcatDataset(train_datasets)
    val_dataset = torch.utils.data.ConcatDataset(val_datasets)

    # Define the training arguments
    training_args = TrainingArguments(
        output_dir='./results',
        per_device_train_batch_size=16,
        per_device_eval_batch_size=64,
        warmup_steps=500,
        weight_decay=0.01,
        logging_dir='./logs',
    )

    # Initialize the trainer
    model = AutoModelForQuestionAnswering.from_pretrained(model_type)
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
    )

    # Define the objective function for Optuna
    def objective(trial):
        # Define a search space
        training_args.learning_rate = trial.suggest_float("learning_rate", 1e-5, 1e-3, log=True)
        training_args.num_train_epochs = trial.suggest_int("num_train_epochs", 1, 5)
        training_args.per_device_train_batch_size = trial.suggest_categorical("per_device_train_batch_size", [8, 16, 32])
        training_args.warmup_steps = trial.suggest_int("warmup_steps", 0, 500)

        # Train the model
        trainer.train()

        # Evaluate the model
        eval_result = trainer.evaluate()

        # Return the evaluation loss
        return eval_result["eval_loss"]

    # Create an Optuna study and optimize the hyperparameters
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=50)

    # Print the best hyperparameters
    print("Best trial:")
    trial = study.best_trial
    print(" Value: ", trial.value)
    print(" Params: ")
    for key, value in trial.params.items():
        print(f"    {key}: {value}")

    # Train the model with the best hyperparameters
    model = AutoModelForQuestionAnswering.from_pretrained(model_type)
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
    )
    trainer.train()

    # Save the model
    if auto is None:
        model_path = input("Enter the path to save the trained model: ")
    trainer.save_model(model_path)

    # Save the tokenizer and the model's configuration
    tokenizer.save_pretrained(model_path)
    model.config.save_pretrained(model_path)

    if auto is None:
        import sys
        python = sys.executable
        os.execl(python, python, *sys.argv)

    else:
        return None
