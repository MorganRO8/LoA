def Inference():

    import os
    import math
    from transformers import BertTokenizer
    from chunk import chunkedinf
    from chunk import chunkedinfoffs

    # make sure the answers directory exists

    try:
        os.mkdir(os.getcwd() + "/answers/")

    except:
        None


    # open tokenizer
    tokenizer = BertTokenizer.from_pretrained('bert-large-uncased-whole-word-masking-finetuned-squad')

    # create question
    questions = []

    while True:
        question = input("Please enter a question(use [answer] in your question to use previous answer in question: ")
        questions.append(question)

        another = input("Would you like to input another question? (y/n) ")
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

                answer, average_value = chunkedinf(tokenizer, full_start_token, z, question)

                # add answer to list
                answer_list.append(answer)

                # add average value to list
                averages_list.append(average_value)

            for z in range(1, num_chunks - 1):

                answer, average_value = chunkedinfoffs(tokenizer, full_start_token, z, question)

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
