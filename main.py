import os
import sys
import json
import datetime
import builtins

# Check for and create the folders used by the program if necessary
os.makedirs(os.path.join(os.getcwd(), 'scraped_docs'), exist_ok=True)
os.makedirs(os.path.join(os.getcwd(), 'dataModels'), exist_ok=True)
os.makedirs(os.path.join(os.getcwd(), 'logs'), exist_ok=True)

builtins.a = os.path.join(os.getcwd(), "logs", f"{str(datetime.datetime.now()).replace(' ', '_')}.txt")
from src.utils import print

def main():
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
    
    # initialize variables
    task = 0

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
                    from src import scrape
                    scrape(task_params)

                elif task_name.lower() == "extract":
                    from src.extract import extract
                    extract(task_params)

                else:
                    print(f"Unknown task type: {task_name}")
                    sys.exit(1)
                    
    else:   
        while task != 1 and task != 2 and task != 3:
            # prompt the user to select a task
            print("Please select a task:")
            print("1. Scrape Papers")
            print("2. Define a CSV Structure")
            print("3. Extract Data from Papers into Defined CSV Structure")

            # get user input
            task = input("Enter the task number (1, 2, or 3): ")

            # check user input and perform the selected task

            if task == "1":
                from src.scrape import scrape
                scrape({})

            elif task == "2":
                from src.meta_model import UI_schema_creator      
                UI_schema_creator()

            elif task == "3":
                print("Loading models, please wait...")
                from src.extract import extract
                extract({})
                
            else:
                print("Invalid task number. Please enter 1 to 3")

if __name__ == '__main__':
    main()
