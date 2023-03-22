from Inference import *
from Scrape import *
import sys
import subprocess
import os

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

    tnum = 0

    # Open the file in read mode
    with open(str(os.getcwd) + "/automatic.txt", "r") as auto:

        # Loop over each line in the file
        for line in auto:
            # Check if the line contains either "task = scrape" or "task = answer"
            if "task = scrape" in line:
                #update the task number
                tnum = tnum + 1
                #get the rest of the variables
                with open(str(os.getcwd) + "/automatic.txt") as command:

                    commands = command.readlines()

                    #Find the index of the corresponding line
                    count = 0
                    for i, line in enumerate(command):
                        if "task =" in line:
                            count += 1
                            if count == tnum:
                                task_index = i
                                break
                    else:
                        raise ValueError(f"The file does not contain at least {tnum} lines with 'task ='")

                    # If "task =" is not found in the file, raise an exception
                    if task_index is None:
                        raise ValueError("The file does not contain any line with 'task ='")

                    # Assign variables from the next 7 lines after the line containing "task ="
                    updownyn = commands[task_index + 1].split("= ")[1].strip()
                    search_terms = commands[task_index + 2].split("= ")[1].strip()
                    scholar_bool = commands[task_index + 3].split("= ")[1].strip()
                    scholar_query = commands[task_index + 4].split("= ")[1].strip()
                    regdlyn = commands[task_index + 5].split("= ")[1].strip()
                    scihubyn = commands[task_index + 6].split("= ")[1].strip()
                    fixbool = commands[task_index + 7].split("= ")[1].strip()

                Scrape(updownyn, search_terms, scholar_bool, scholar_query, regdlyn, scihubyn, fixbool, sys.argv)
            elif "task = answer" in line:
                # update the task number
                tnum = tnum + 1
                # get the rest of the variables
                with open(str(os.getcwd) + "/automatic.txt") as command:

                    commands = command.readlines()

                    # Find the index of the corresponding line
                    count = 0
                    for i, line in enumerate(command):
                        if "task =" in line:
                            count += 1
                            if count == tnum:
                                task_index = i
                                break
                    else:
                        raise ValueError(f"The file does not contain at least {tnum} lines with 'task ='")

                    # If "task =" is not found in the file, raise an exception
                    if task_index is None:
                        raise ValueError("The file does not contain any line with 'task ='")

                    # Assign variables from the next 7 lines after the line containing "task ="
                    model_name = commands[task_index + 1].split("= ")[1].strip()
                    questions = []
                    exec("questions = " + commands[task_index + 2].split("= ")[1].strip())
                    selected_dir = commands[task_index + 3].split("= ")[1].strip()
                    xlyn = commands[task_index + 4].split("= ")[1].strip()

                Inference(model_name, questions, selected_dir, xlyn, sys.argv)

            elif "#end" in line:
                sys.exit()

            else:
                None

# prompt the user to select a task
print("Please select a task:")
print("1. Install Dependencies")
print("2. Scrape Papers")
print("3. Run Inference")

# get user input
task = input("Enter the task number (1, 2, or 3): ")

# check user input and perform the selected task

if task == "1":
    print("Installing dependencies...")

    # Define the packages and versions to install
    packages = [
        'torch==1.13.1',
        'transformers==4.26.0',
        'paperscraper==0.2.4',
        'PyPDF2==3.0.1',
        'scidownl==1.0.0'
        'pandas==1.5.3'
    ]

    # Loop through the packages and run 'pip install' command
    for package in packages:
        subprocess.run(['pip', 'install', package])

elif task == "2":
    print("Checking for -rxiv dumps...")
    Scrape()

elif task == "3":
    print("Running inference...")
    Inference()

else:
    print("Invalid task number. Please enter 1, 2, or 3.")

    python = sys.executable
    os.execl(python, python, *sys.argv)
