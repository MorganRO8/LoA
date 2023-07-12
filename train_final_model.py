import os

import pandas as pd
from pytorch_lightning import Trainer
from torch.utils.data import DataLoader

from rwkv.src.dataset import MyDataset
from rwkv.src.model import RWKV
from utils import select_csv, get_out_id


def train_final_model(args):
    auto = args.get('auto')
    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')
    n_epochs = args.get('n_epochs')

    if auto is None:
        # Have user select scrape results to use
        directory = select_csv()
        output_directory_id = directory.replace(str(os.path.join(os.getcwd(), 'snorkel')), '', 1)
        n_epochs = input("Please enter the number of epochs you would like to train for:")
    else:
        output_directory_id, garbo = get_out_id(def_search_terms, maybe_search_terms)
        directory = os.path.join(os.getcwd(), 'snorkel', output_directory_id)

    # Load the csv file
    df = pd.read_csv(os.path.join(directory, 'final_qa_data.csv'))

    # Convert dataframe to text
    df_text = df.to_csv(sep=' ', index=False)

    # Save the text to a jsonl file
    jsonl_file = os.path.join(directory, 'final_qa_data.jsonl')
    with open(jsonl_file, 'w') as file:
        file.write(df_text)

    # Convert the jsonl file to .bin and .idx using json2binidx_tool
    # Note: You need to have the 20B_tokenizer.json file in the current directory
    os.system(
        f'python tools/preprocess_data.py --input {jsonl_file} --output-prefix {os.path.join(directory, "final_qa_data")} --vocab ./20B_tokenizer.json')

    # Load the dataset
    train_data = MyDataset(os.path.join(directory, 'final_qa_data.bin'), os.path.join(directory, 'final_qa_data.idx'))

    # Load the pretrained model
    model = RWKV.from_pretrained(os.path.join(os.getcwd(), 'pretrain', output_directory_id))

    # Set up the trainer
    trainer = Trainer(max_epochs=n_epochs, gpus=1)

    # Set up the dataloader
    dataloader = DataLoader(train_data, batch_size=32, num_workers=4)

    # Train the model
    trainer.fit(model, dataloader)

    # Save the trained model
    model.save_pretrained(os.path.join(os.getcwd(), 'trained', output_directory_id))
