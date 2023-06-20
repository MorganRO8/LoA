#!/bin/bash
#SBATCH -t 96:00:00
#SBATCH --job-name chromophore_search
#SBATCH --gres=gpu:1
#SBATCH --mem=500GB
#SBATCH -q express
#SBATCH -p earwp
#SBATCH -N 1
#SBATCH -n 3
#SBATCH -o log.out
#SBATCH -e err

module load cuda/11.0

# Check if conda is installed
if ! command -v conda &> /dev/null
then
    echo "conda not found, installing Miniconda"
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda
    rm Miniconda3-latest-Linux-x86_64.sh
    echo "export PATH=\${PATH}:\${HOME}/miniconda/bin" >> \${HOME}/.bashrc
    export PATH=\${PATH}:\${HOME}/miniconda/bin
    conda init bash
else
	source /wsu/home/gi/gi16/gi1632/.bashrc
	conda init bash
fi

# Check if the conda environment exists
if conda env list | grep -q 'LoA'
then
    echo "Conda environment LoA exists"
else
    echo "Creating conda environment LoA"
    conda env create -f environment.yml
fi

# Activate the conda environment
conda activate LoA

# Check if the conda environment was activated successfully
if [[ $(conda env list | grep '*' | awk '{print $1}') != "LoA" ]]
then
    echo "Failed to activate conda environment LoA"
    exit
fi

# Check if EDirect is installed
if ! command -v esearch &> /dev/null
then
    echo "Installing EDirect"
    wget -q ftp://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/install-edirect.sh
    sh install-edirect.sh -y
    rm install-edirect.sh
    echo "export PATH=\${PATH}:\${HOME}/edirect" >> \${HOME}/.bashrc
    export PATH=\${PATH}:\${HOME}/edirect
fi

# Check if Detectron2 is installed
if ! pip show detectron2 &> /dev/null
then
    echo "Installing Detectron2"
    git clone https://github.com/facebookresearch/detectron2.git
    pip install -e detectron2
fi

# Activate the LoA environment and run the Python script
source activate LoA && python main.py -auto