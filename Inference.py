def Inference():

    import os
    import math
    import transformers
    from transformers import AutoTokenizer
    from chunk import chunkedinf
    from chunk import chunkedinfoffs
    import pandas as pd


    # make sure the answers directory exists

    try:
        os.mkdir(os.getcwd() + "/answers/")

    except:
        None

    def download_model():
        model_name = input("Please provide the name of the huggingface model you'd like to use: ")
        filepath = f"models/{model_name}"
        if not os.path.exists("models"):
            os.mkdir("models")
        if os.path.exists(filepath):
            print("The model already exists in the models directory.")
            return model_name
        model = transformers.AutoModel.from_pretrained(model_name)
        model.save_pretrained(filepath)
        print("Model downloaded successfully.")
        return model_name

    def select_model():
        models_dir = "models/"
        if not os.path.exists(models_dir):
            os.mkdir(models_dir)
        subdirs = []
        for dirpath, dirnames, filenames in os.walk(models_dir):
            for dirname in dirnames:
                subdir_path = os.path.join(dirpath, dirname)
                for subdirname in os.listdir(subdir_path):
                    subsubdir_path = os.path.join(subdir_path, subdirname)
                    if os.path.isdir(subsubdir_path):
                        subdirs.append(subsubdir_path)
        if not subdirs:
            print("No models found in the models directory.")
            return download_model()
        print("The following models are available:")
        for i, subdir in enumerate(subdirs):
            print(f"{i + 1}: {subdir}")
        while True:
            choice = input("Enter the number of the model you'd like to use or 'd' to download a new model: ")
            if choice == "d":
                return download_model()
            try:
                choice = int(choice)
                if 1 <= choice <= len(subdirs):
                    model_dir = subdirs[choice - 1]
                    model_files = [os.path.join(model_dir, f) for f in os.listdir(model_dir) if
                                   os.path.isfile(os.path.join(model_dir, f))]
                    if model_files:
                        return model_dir
                    else:
                        print(f"No models found in {model_dir} directory.")
                else:
                    print("Invalid choice. Please try again.")
            except ValueError:
                print("Invalid choice. Please try again.")

    dlyn = input("Would you like to download a new model or use an existing one?(d/e)").lower()

    if dlyn == "d":
        model_name = download_model()

    elif dlyn == "e":
        model_name = select_model()

    else:
        print("You must select d or e")

    # open tokenizer
    tokenizer = AutoTokenizer.from_pretrained(model_name.replace('models/',''), model_type='megatron-bert')

    # create question
    questions = []

    while True:
        question = input("Please enter a question (use [answer] in your question to use previous answer in question): ").lower()
        questions.append(question)

        another = input("Would you like to input another question? (y/n) ").lower()
        if another.lower() == "n":
            break

    print("Here are the questions you entered:")
    for question in questions:
        print(question)

    #define input directory
    import os

    # specify the directory path to list the subdirectories
    directory_path = str(os.getcwd()) + "/txts/"

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

    # get the selected directory path
    selected_dir = subdirectories[int(selected_dir_num) - 1]
    selected_dir_path = os.path.join(directory_path, selected_dir)

    # make specific answers directory
    try:
        os.mkdir(os.getcwd() + "/answers/" + selected_dir)

    except:
        None

    # do something with the selected directory path
    print("You selected:", selected_dir_path)

    with open(selected_dir_path + '/output.txt', 'r') as f:
        lines = f.readlines()

        p = 0

    for question in questions:

        # Check if the user wanted to use the previous answer as part of the next question
        if '[answer]' in question:
            with open(os.getcwd() + "/answers/" + selected_dir + 'answers.txt') as f:
                answers = f.readlines()

            # Get the last answer from the list (strip any leading/trailing whitespace)
            last_answer = answers[-1].strip()

            question = question.replace('[answer]', last_answer)

        for line in lines:
            p = p + 1
            total_text = line.strip()

            # tokenize text
            full_start_token = tokenizer.encode(total_text)

            # calculate number of chunks
            num_chunks = math.ceil(len(full_start_token) / 400)

            # create list to store average values
            averages_list = []

            # create list to store answers
            answer_list = []


            # loop through chunks
            for z in range(1, num_chunks):

                answer, average_value = chunkedinf(tokenizer, full_start_token, z, question, model_name)

                # add answer to list
                answer_list.append(answer)

                # add average value to list
                averages_list.append(average_value)

            for z in range(1, num_chunks - 1):

                answer, average_value = chunkedinfoffs(tokenizer, full_start_token, z, question, model_name)

                # add answer to list
                answer_list.append(answer)

                # add average value to list
                averages_list.append(average_value)

            # find maximum average value
            average_max = max(averages_list)

            # find index of maximum average value
            best_index = averages_list.index(average_max)

            # find best answer
            best_answer = answer_list[best_index]

            # open output file

            with open(os.getcwd() + "/answers/" + selected_dir + 'answers.txt', 'w') as g:

                # write best answer to output file
                g.write(best_answer + '\n')

    print('Done!')

    xlyn = input("Would you like to convert answers to excel format?(y/n)").lower()

    if xlyn == "y":
        # Read in the answers from the text file
        with open(os.getcwd() + "/answers/" + selected_dir + 'answers.txt', "r") as f:
            answers = f.readlines()

        # Create an empty dictionary to hold the answers for each question
        question_answers = {q: [] for q in questions}

        # Loop through each answer and append it to the corresponding question's list
        for i, ans in enumerate(answers):
            question_answers[questions[i % len(questions)]].append(ans.strip())

        # Create a DataFrame from the question_answers dictionary
        df = pd.DataFrame(question_answers)

        # Write the DataFrame to an Excel file
        output_file = "output.xlsx"
        df.to_excel(output_file, index=False)

    elif xlyn == "n":
        None

    else:
        print("You must select y or n")

    import sys
    python = sys.executable
    os.execl(python, python, *sys.argv)
