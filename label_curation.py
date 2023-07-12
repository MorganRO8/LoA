import os

import pandas as pd

from utils import select_csv, get_out_id


def label_curation(args):
    auto = args.get('auto')
    def_search_terms = args.get('def_search_terms')
    maybe_search_terms = args.get('maybe_search_terms')

    if auto is None:
        csv_folder = select_csv()

    else:
        output_directory_id, trash = get_out_id(def_search_terms, maybe_search_terms)
        csv_folder = os.path.join(os.getcwd(), 'snorkel', output_directory_id)

    df = pd.read(f"{csv_folder}/initial_label_data.csv")

    # Iterate over the rows of the DataFrame
    for i, row in df.iterrows():
        # Present the user with the row
        print(row)

        # Ask the user to confirm or deny the row
        while True:
            user_input = input("Is this row correct? (yes/no): ")
            if user_input.lower() in ['yes', 'no']:
                break
            print("Invalid input. Please enter 'yes' or 'no'.")

        # If the row is not correct, ask the user which column to replace and what to replace it with
        if user_input.lower() == 'no':
            while True:
                column = input("Which column number would you like to replace? ")
                if column.isdigit() and int(column) < len(df.columns):
                    break
                print("Invalid input. Please enter a valid column number.")
            replacement = input("What is the replacement string? ")
            df.iat[i, int(column)] = replacement

        # Ask the user if they want to continue
        while True:
            user_input = input("Do you want to continue? (yes/no): ")
            if user_input.lower() in ['yes', 'no']:
                break
            print("Invalid input. Please enter 'yes' or 'no'.")

        # If the user doesn't want to continue, break the loop
        if user_input.lower() == 'no':
            break

    pd.to_csv(os.path.join(csv_folder, 'curated_label_data.csv'), index=False)
