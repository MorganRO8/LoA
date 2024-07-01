import os
import sys
import json
import datetime
import builtins

# Create necessary directories if they don't exist
os.makedirs(os.path.join(os.getcwd(), 'scraped_docs'), exist_ok=True)
os.makedirs(os.path.join(os.getcwd(), 'dataModels'), exist_ok=True)
os.makedirs(os.path.join(os.getcwd(), 'logs'), exist_ok=True)

# Set up custom logging
# This creates a log file with the current timestamp in the filename
builtins.a = os.path.join(os.getcwd(), "logs", f"{str(datetime.datetime.now()).replace(' ', '_')}.txt")
from src.utils import print  # Custom print function for logging


def main():
    # Print ASCII art banner
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

    # Initialize task variable
    task = 0

    # Check if the script is run in automatic mode
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

        # Open the JSON file and parse its contents
        with open(os.path.join(os.getcwd(), json_file), "r") as auto:
            tasks = json.load(auto)

            # Loop over each task in the file
            for task_name, task_params in tasks.items():
                if task_name.lower() == "scrape":
                    from src.scrape import scrape
                    scrape(task_params)

                elif task_name.lower() == "extract":
                    from src.extract import batch_extract
                    batch_extract(task_params)

                elif task_name.lower() == 'concurrent':
                    from src.single_paper import scrape_and_extract_concurrent
                    scrape_and_extract_concurrent(task_params)

                else:
                    print(f"Unknown task type: {task_name}")
                    sys.exit(1)

    else:
        # Interactive mode
        while task not in [1, 2, 3, 4]:
            # Prompt the user to select a task
            print("Please select a task:")
            print("1. Scrape Papers")
            print("2. Define a CSV Structure")
            print("3. Extract Data from Papers into Defined CSV Structure")
            print("4. Scrape and Extract Concurrently")

            # Get user input
            task = input("Enter the task number (1, 2, 3, or 4): ")

            # Execute the selected task based on user input
            if task == "1":
                from src.scrape import scrape
                scrape({})

            elif task == "2":
                from src.meta_model import UI_schema_creator
                UI_schema_creator()

            elif task == "3":
                print("Loading models, please wait...")
                from src.extract import batch_extract
                batch_extract({})

            elif task == "4":
                from src.single_paper import scrape_and_extract_concurrent
                scrape_and_extract_concurrent({})

            else:
                print("Invalid task number. Please enter 1 to 4")


if __name__ == '__main__':
    main()