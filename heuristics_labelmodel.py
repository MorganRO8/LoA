# Imports
import re
from functools import partial
import nltk
import pandas as pd
import spacy
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from snorkel.labeling import PandasLFApplier
from snorkel.labeling.model import LabelModel
from snorkel.types import DataPoint
from transformers import pipeline

from utils import *

# Check if nltk_data directory exists
try:
    nltk.data.find('corpora/wordnet')
    print("WordNet is already downloaded.")
except LookupError:
    print("WordNet is not downloaded. Downloading now...")
    nltk.download('wordnet')
from nltk.corpus import wordnet


# Modify the original function with function names
def heuristics_labelmodel(args):
    tasks = args.get('tasks')
    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')
    model_type = args.get('model_type')
    auto = args.get('auto')

    if auto is None:
        csv_folder = select_csv()

    else:
        output_directory_id, trash = get_out_id(def_search_terms, maybe_search_terms)
        csv_folder = os.path.join(os.getcwd(), 'snorkel', output_directory_id)

    os.makedirs(csv_folder, exist_ok=True)  # create the directory if it doesn't exist

    # Definitions

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

    if auto is None:
        # Have user select scrape results to use
        output_directory_id = select_scrape_results()

    # Initialization
    nlp = spacy.load('en_core_web_sm')
    if auto is None:
        tasks = []
        task_count = int(input("Enter the number of tasks: ")) if tasks is None else len(tasks)
        for i in range(task_count):
            task_name = input(f"Enter the name of task {i + 1}: ")
            keywords = get_keywords(None)
            regexes = input(
                "Please input any regular expressions you would like to use as labels, separated by triple bars (|||):").split(
                '|||')
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
            regexes = task.get('regexes')
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
    texts = load_text_files(os.getcwd() + '/txts/' + output_directory_id, model_type)

    # Convert texts to DataFrame
    df_train = pd.DataFrame(texts, columns=['text'])

    # Instantiate lfs list
    lfs = []

    def lf_regex_search(x: DataPoint, regexes) -> int:
        for regex in regexes:
            if re.search(regex, x.text):
                return 1
        return 0

    # Define the labeling functions outside the loop
    def lf_keyword_search(x: DataPoint, keyword) -> int:
        return 1 if keyword in x.text.lower() else 0

    def lf_question_answering(x: DataPoint, nlp, question) -> int:
        answer = nlp(question=question, context=x.text)
        if answer == 'yes':
            if answer['score'] > 0.5:
                return 1
        else:
            return 0

    def lf_sentence_similarity(x: DataPoint, model, sentence_embedding) -> int:
        x_embedding = model.encode([x.text])
        print(f"Sentence embedding dims: {sentence_embedding.ndim}")
        print(f"x_embedding dims: {x_embedding.ndim}")
        similarity = cosine_similarity(sentence_embedding, x_embedding)[0][0]
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
            def named_lf_keyword_search(x: DataPoint) -> int:
                return lf_keyword_search(x, keyword=keyword)

            named_lf_keyword_search.name = f"lf_keyword_search_{keyword}".replace(" ", "_")
            lfs.append(named_lf_keyword_search)

        for model_id in task['model_identifiers']:
            for question in task['questions']:
                question = question + " Answer simply yes or no."
                nlpr = pipeline('question-answering', model=model_id)

                def named_lf_question_answering(x: DataPoint) -> int:
                    return lf_question_answering(x, nlp=nlpr, question=question)

                named_lf_question_answering.name = f"lf_question_answering_{model_id}_{question}".replace(" ", "_")
                lfs.append(named_lf_question_answering)

        model = SentenceTransformer("sentence-transformers/LaBSE")

        for sentence in task['sentences']:
            sentence_embedding = model.encode([sentence])

            def named_lf_sentence_similarity(x: DataPoint) -> int:
                return lf_sentence_similarity(x, model=model, sentence_embedding=sentence_embedding)

            named_lf_sentence_similarity.name = f"lf_sentence_similarity_{model_id}_{sentence}".replace(" ", "_")
            lfs.append(named_lf_sentence_similarity)

            def named_lf_sentence_matching(x: DataPoint) -> int:
                return lf_sentence_matching(x, sentence=sentence)

            named_lf_sentence_matching.name = f"lf_sentence_matching_{sentence}".replace(" ", "_")
            lfs.append(named_lf_sentence_matching)

        def make_lf_regex_search(regex):
            def lf(document):
                return CLASS if re.search(regex, document.text) else ABSTAIN

            # Sanitize the regex and use it as the function name
            sanitized_regex = regex.replace("\\", "").replace("(", "").replace(")", "").replace("+", "").replace("*",
                                                                                                                 "").replace(
                " ", "_")
            lf.name = f"lf_regex_search_{task}_{sanitized_regex}"
            return lf

        # Create the labeling functions
        for regex in regexes:
            lfs.append(make_lf_regex_search(regex))

        # Debug print for lfs
        print(f"There are {len(lfs)} labeling functions")
        print("Labeling function names:")
        print()
        for lf in lfs:
            print(lf)
            print(lf.name)
            print()

        # File paths for the CSV file and model checkpoint
        csv_file_path = os.path.join(csv_folder, f"{task['name']}_label_data.csv")
        model_file_path = os.path.join(csv_folder, f"{task['name']}_label_model.pkl")

        # Check if a CSV file exists for the task
        if os.path.isfile(csv_file_path) and os.path.isfile(model_file_path):
            print(f"CSV file and model file found for task {task['name']}. Skipping.")

        elif os.path.isfile(csv_file_path) and not os.path.isfile(model_file_path):
            print(f"CSV file found for task {task['name']}, but no model file found. Training on data.")

            # Load the dataframe from the CSV file
            df_train = pd.read_csv(csv_file_path)

            # Apply the labeling functions to your dataset
            applier = PandasLFApplier(lfs)
            L_train = applier.apply(df_train)

            # Train a Snorkel LabelModel to combine the labels
            label_model = LabelModel(cardinality=2, verbose=True)
            label_model.fit(L_train, n_epochs=5000, log_freq=100, seed=123, lr=0.001, lr_scheduler="linear")

            # Save the model to a checkpoint
            label_model.save(model_file_path)

            # Transform the labels into a single set of noise-aware probabilistic labels
            df_train[task['name'] + "_label"] = label_model.predict(L=L_train, tie_break_policy="abstain")

            # Add a column for positive results for each task
            df_train[task['name'] + "_positive"] = df_train[task['name'] + "_label"].apply(
                lambda x: x if x == 1 else None)

            # Save the dataframe to a CSV file
            df_train.to_csv(csv_file_path, index=False)

        else:
            print(f"No model or csv file found for {task['name']}, starting from scratch.")

            # Apply the labeling functions to your dataset
            applier = PandasLFApplier(lfs)
            L_train = applier.apply(df_train)

            # Train a Snorkel LabelModel to combine the labels
            label_model = LabelModel(cardinality=2, verbose=True)
            label_model.fit(L_train, n_epochs=5000, log_freq=100, seed=123, lr=0.001, lr_scheduler="linear")

            # Save the model to a checkpoint
            label_model.save(model_file_path)

            # Transform the labels into a single set of noise-aware probabilistic labels
            df_train[task['name'] + "_label"] = label_model.predict(L=L_train, tie_break_policy="abstain")

            # Add a column for positive results for each task
            df_train[task['name'] + "_positive"] = df_train[task['name'] + "_label"].apply(
                lambda x: x if x == 1 else None)

            # Save the DataFrame to a CSV file
            df_train.to_csv(os.path.join(csv_folder, 'initial_label_data.csv'), index=False)
