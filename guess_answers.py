# Imports
import optuna
import pandas as pd
import torch
from sklearn.model_selection import train_test_split
from torch.utils.data import Subset
from transformers import AutoModelForQuestionAnswering, TrainingArguments, Trainer, pipeline, \
    AutoModelForSequenceClassification

from utils import *


def guess_answers(args):
    tasks = args.get('tasks')
    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')
    model_type = args.get('model_type')
    model_path = args.get('model_path')
    auto = args.get('auto')

    if auto is None:
        csv_folder = select_csv()

    else:
        output_directory_id, trash = get_out_id(def_search_terms, maybe_search_terms)
        csv_folder = os.path.join(os.getcwd(), 'snorkel', output_directory_id)

    # Load the DataFrame from the CSV file
    df_train = pd.read_csv(csv_folder + 'curated_label_data.csv')

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
        training_args.per_device_train_batch_size = trial.suggest_categorical("per_device_train_batch_size",
                                                                              [8, 16, 32])
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

    # Load text files
    texts = load_text_files(os.getcwd() + '/txts/' + output_directory_id, model_type)

    # Convert texts to DataFrame
    df_train = pd.DataFrame(texts, columns=['text'])

    # Initialize an empty DataFrame to store the results
    df_results = pd.DataFrame(columns=['text_span'] + [task['name'] + '_answer' for task in tasks])

    # Loop over each row in the DataFrame
    for index, row in df_train.iterrows():
        text_span = row['text']

        # Initialize a dictionary to store the results for this text span
        results = {'text_span': text_span}

        # Loop over each task
        for task in tasks:
            question = task.get('question')
            model_identifier = task.get('model')

            # Load the trained label model
            tokenizer = AutoTokenizer.from_pretrained(model_path)
            label_model = AutoModelForSequenceClassification.from_pretrained(model_path)

            # Apply the label model to the text span
            inputs = tokenizer(text_span, return_tensors='pt')
            outputs = label_model(**inputs)
            _, predicted = torch.max(outputs.logits, 1)

            # If the label model predicts that the text span contains an answer, apply the extraction model
            if predicted.item() == 1:
                # Initialize a question answering pipeline with the extraction model
                nlp = pipeline('question-answering', model=model_identifier)

                # Use the pipeline to extract an answer from the text span
                result = nlp(question=question, context=text_span)

                # Store the extracted answer in the results dictionary
                results[task['name'] + '_answer'] = result['answer']

        # Append the results for this text span to the DataFrame
        df_results = df_results.append(results, ignore_index=True)

    # Save the DataFrame to a CSV file
    df_results.to_csv(os.path.join(csv_folder, 'guess_answers.csv'), index=False)

    if auto is None:
        import sys
        python = sys.executable
        os.execl(python, python, *sys.argv)

    else:
        return None
