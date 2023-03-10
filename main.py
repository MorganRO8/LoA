from Inference import *
from Scrape import *

# prompt the user to select a task
print("Please select a task:")
print("1. Scrape Papers")
print("2. Run Inference")

# get user input
task = input("Enter the task number (1 or 2): ")

# check user input and perform the selected task
if task == "1":
    print("Checking for -rxiv dumps...")
    Scrape()
elif task == "2":
    print("Running inference...")
    Inference()
else:
    print("Invalid task number. Please enter 1 or 2.")