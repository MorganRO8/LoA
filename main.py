from Inference import *
from Scrape import *
import sys
import subprocess
import os

# prompt the user to select a task
print("Please select a task:")
print("1. Install dependencies")
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

if task == "2":
    print("Checking for -rxiv dumps...")
    Scrape()
elif task == "3":
    print("Running inference...")
    Inference()
else:
    print("Invalid task number. Please enter 1, 2, or 3.")

    python = sys.executable
    os.execl(python, python, *sys.argv)
