import json
import os
import sys

# Set env variables for rwkv here, because they need to be loaded before the package is imported

os.environ["RWKV_JIT_ON"] = '1'
os.environ["RWKV_CUDA_ON"] = '1'
os.environ["RWKV_T_MAX"] = f'{int(2 ^ 18)}'
os.environ["RWKV_FLOAT_MODE"] = "fp32"


def main():
    from answer_curation import answer_curation
    from doc_to_txt import doc_to_txt
    from guess_answers import guess_answers
    from heuristics_labelmodel import heuristics_labelmodel
    from inference import inference
    from label_curation import label_curation
    from scrape import scrape
    from train_final_model import train_final_model

    print("""
    
              _____           _______                   _____          
             /\    \         /::\    \                 /\    \         
            /::\____\       /::::\    \               /::\    \        
           /:::/    /      /::::::\    \             /::::\    \       
          /:::/    /      /::::::::\    \           /::::::\    \      
         /:::/    /      /:::/~~\:::\    \         /:::/\:::\    \     
        /:::/    /      /:::/    \:::\    \       /:::/__\:::\    \    
       /:::/    /      /:::/    / \:::\    \     /::::\   \:::\    \   
      /:::/    /      /:::/____/   \:::\____\   /::::::\   \:::\    \  
     /:::/    /      |:::|    |     |:::|    | /:::/\:::\   \:::\    \ 
    /:::/____/       |:::|____|     |:::|    |/:::/  \:::\   \:::\____\
    
    \:::\    \        \:::\    \   /:::/    / \::/    \:::\  /:::/    /
     \:::\    \        \:::\    \ /:::/    /   \/____/ \:::\/:::/    / 
      \:::\    \        \:::\    /:::/    /             \::::::/    /  
       \:::\    \        \:::\__/:::/    /               \::::/    /   
        \:::\    \        \::::::::/    /                /:::/    /    
         \:::\    \        \::::::/    /                /:::/    /     
          \:::\    \        \::::/    /                /:::/    /      
           \:::\____\        \::/____/                /:::/    /       
            \::/    /         ~~                      \::/    /        
             \/____/                                   \/____/         
    """)

    if "-auto" in sys.argv:
        # Get the index of the -auto argument
        auto_index = sys.argv.index("-auto")
        # Check if there is another argument after -auto
        if len(sys.argv) > auto_index + 1:
            # Use the next argument as the path to the JSON file
            json_file = sys.argv[auto_index + 1]
        else:
            # If there is no argument after -auto, default to automatic.json
            json_file = "automatic.json"

        # Open the file in read mode
        with open(os.path.join(os.getcwd(), json_file), "r") as auto:
            # Parse the JSON file
            tasks = json.load(auto)

            # Loop over each task in the file
            for task_name, task_params in tasks.items():
                if task_name.lower() == "scrape":
                    scrape(task_params)

                elif task_name.lower() == "doc_to_txt":
                    doc_to_txt(task_params)

                elif task_name.lower() == "heuristics_labelmodel":
                    heuristics_labelmodel(task_params)

                elif task_name.lower() == "label_curation":
                    label_curation(task_params)

                elif task_name.lower() == "guess_answers":
                    guess_answers(task_params)

                elif task_name.lower() == "answer_curation":
                    answer_curation(task_params)

                elif task_name.lower() == "train_final_model":
                    train_final_model(task_params)

                elif task_name.lower() == "inference":
                    inference(task_params)

                else:
                    print(f"Unknown task type: {task_name}")
                    sys.exit(1)

    # prompt the user to select a task
    print("Please select a task:")
    print("1. Scrape Papers")
    print("2. Convert Scraped Docs to Plaintext")
    print("3. Label Data with Heuristics")
    print("4. Curate labels")
    print("5. Have a Model Guess Answers from Labelled Data")
    print("6. Curate Answers")
    print("7. Train QA Model")
    print("8. Run Question Answering")

    # get user input
    task = input("Enter the task number (1 to 8): ")

    # check user input and perform the selected task

    if task == "1":
        scrape({})

    elif task == "2":
        doc_to_txt({})

    elif task == "3":
        heuristics_labelmodel({})

    elif task == "4":
        label_curation({})

    elif task == "5":
        guess_answers({})

    elif task == "6":
        answer_curation({})

    elif task == "7":
        train_final_model({})

    elif task == "8":
        inference({})

    else:
        print("Invalid task number. Please enter 1 to 8")

        python = sys.executable
        os.execl(python, python, *sys.argv)


main()
