#!/bin/bash
#SBATCH -t 96:00:00
#SBATCH --job-name ~LoA~
#SBATCH --gres=gpu:1
#SBATCH --mem=24GB
#SBATCH -q gpu
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -o "./logs/latest.txt"
#SBATCH -e "./logs/latest_err.txt"

module load cuda/11.0

source /wsu/home/gi/gi16/gi1632/.bashrc
conda init bash

conda activate LoA-v3

python main.py -auto "./job_scripts/extract_test.json"