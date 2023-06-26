from Inference import *
from Scrape import *
from snorkel_train import *
import sys
import os
import json

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
                Scrape(task_params)

            elif task_name.lower() == "snorkel_train":
                snorkel_train(task_params)

            elif task_name.lower() == "inference":
                Inference(task_params)

            else:
                print(f"Unknown task type: {task_name}")
                sys.exit(1)


# prompt the user to select a task
print("Please select a task:")
print("1. Scrape Papers")
print("2. Train a model")
print("3. Run Question Answering")

# get user input
task = input("Enter the task number (1, 2, or 3): ")

# check user input and perform the selected task

if task == "1":
    Scrape({})

elif task == "2":
    snorkel_train({})

elif task == "3":
    Inference({})

else:
    print("Invalid task number. Please enter 1, 2, or 3.")

    python = sys.executable
    os.execl(python, python, *sys.argv)
