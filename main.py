from Inference import *
from Scrape import *
from snorkel_train import *
import sys
import subprocess
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
    # Open the file in read mode
    with open(str(os.getcwd()) + "/automatic.json", "r") as auto:
        # Parse the JSON file
        tasks = json.load(auto)

        # Loop over each task in the file
        for task in tasks:
            task_type = task.get('task_type')
            task_vars = task.get('task_vars')

            if task_type == "scrape":
                Scrape(**task_vars)

            elif task_type == "train":
                snorkel_train(task_vars, sys.argv)

            elif task_type == "answer":
                Inference(**task_vars)

            else:
                print(f"Unknown task type: {task_type}")
                sys.exit(1)

# prompt the user to select a task
print("Please select a task:")
print("1. Install Dependencies")
print("2. Scrape Papers")
print("3. Train a model")
print("4. Run Question Answering")

# get user input
task = input("Enter the task number (1, 2, or 3): ")

# check user input and perform the selected task

if task == "1":
    print("Installing dependencies...")

    conda_packages = [
        "selenium",
        "beautifulsoup4",
        "requests",
        "spacy",
        "pandas",
        "pytorch",
        "nltk",
        "transformers",
        "scikit-learn",
        "unstructured",
        "pdfminer.six",
        "textwrap3"
    ]

    pip_packages = [
        "webdriver_manager",
        "optuna",
        "snorkel",
        "sentence-transformers"
    ]

    system_dependencies = [
        "libmagic",
        "poppler",
        "tesseract",
        "libreoffice"
    ]

    for package in conda_packages:
        subprocess.check_call(["conda", "install", "-y", package])

    for package in pip_packages:
        subprocess.check_call(["pip", "install", package])

    for dependency in system_dependencies:
        subprocess.check_call(["conda", "install", "-y", "-c", "conda-forge", dependency])

    # Download and run the EDirect installation script
    install_cmd = 'sh -c "$(wget -q ftp://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/install-edirect.sh -O -)"'
    install_result = subprocess.run(install_cmd, shell=True, text=True, stderr=subprocess.PIPE)

    if install_result.returncode != 0:
        print(f"Error occurred while installing EDirect: {install_result.stderr}")
    else:
        print("(ignore message about activating in session, PATH variable updated if necessary)")

    # Add EDirect to the PATH in .bashrc
    update_path_cmd = 'echo "export PATH=${PATH}:${HOME}/edirect" >> ${HOME}/.bashrc'
    update_path_result = subprocess.run(update_path_cmd, shell=True, text=True, stderr=subprocess.PIPE)

    if update_path_result.returncode != 0:
        print(f"Error occurred while updating .bashrc: {update_path_result.stderr}")
    else:
        print("EDirect added to PATH in .bashrc")

    edirect_path = os.path.expanduser("~/edirect")
    path_elements = os.environ["PATH"].split(os.pathsep)

    if edirect_path not in path_elements:
        os.environ["PATH"] += os.pathsep + edirect_path
        print("PATH variable updated for current session")

    import torch
    import os
    import subprocess

    # Get CUDA version
    cuda_version = torch.version.cuda
    # Get torch version
    torch_version = torch.__version__

    # Define the base command for installing detectron2
    base_cmd = "python -m pip install detectron2 -f "

    # Define the URLs for different versions of CUDA and PyTorch
    url_dict = {
        "11.3": {
            "1.10": "https://dl.fbaipublicfiles.com/detectron2/wheels/cu113/torch1.10/index.html"
        },
        "11.1": {
            "1.10": "https://dl.fbaipublicfiles.com/detectron2/wheels/cu111/torch1.10/index.html",
            "1.9": "https://dl.fbaipublicfiles.com/detectron2/wheels/cu111/torch1.9/index.html",
            "1.8": "https://dl.fbaipublicfiles.com/detectron2/wheels/cu111/torch1.8/index.html"
        },
        "10.2": {
            "1.10": "https://dl.fbaipublicfiles.com/detectron2/wheels/cu102/torch1.10/index.html",
            "1.9": "https://dl.fbaipublicfiles.com/detectron2/wheels/cu102/torch1.9/index.html",
            "1.8": "https://dl.fbaipublicfiles.com/detectron2/wheels/cu102/torch1.8/index.html"
        },
        "10.1": {
            "1.8": "https://dl.fbaipublicfiles.com/detectron2/wheels/cu101/torch1.8/index.html"
        },
        "cpu": {
            "1.10": "https://dl.fbaipublicfiles.com/detectron2/wheels/cpu/torch1.10/index.html",
            "1.9": "https://dl.fbaipublicfiles.com/detectron2/wheels/cpu/torch1.9/index.html"
        }
    }

    # Check if the CUDA version is in the dictionary
    if cuda_version in url_dict:
        # Check if the torch version is in the dictionary for the CUDA version
        if torch_version in url_dict[cuda_version]:
            # Get the URL for the correct version of CUDA and PyTorch
            url = url_dict[cuda_version][torch_version]
            # Create the full command
            cmd = base_cmd + url
            # Run the command
            subprocess.run(cmd, shell=True)
        else:
            print(f"Unsupported torch version {torch_version} for CUDA version {cuda_version}")
    else:
        print(f"Unsupported CUDA version {cuda_version}")

    python = sys.executable
    os.execl(python, python, *sys.argv)

elif task == "2":
    Scrape({})

elif task == "3":
    snorkel_train({})

elif task == "4":
    Inference({})

else:
    print("Invalid task number. Please enter 1, 2, or 3.")

    python = sys.executable
    os.execl(python, python, *sys.argv)
