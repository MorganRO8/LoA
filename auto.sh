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

source /wsu/home/gi/gi16/gi1632/.bashrc
conda init bash
conda activate LoA

python main.py -auto