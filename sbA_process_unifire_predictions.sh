#!/bin/bash

#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=72:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=8  # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=64G   # memory per node
#SBATCH -J "UP_Unifire_result_combine"   # job name
#SBATCH -o "console_unifire/UP_UFire_res_com_%A_%a.out"   # job output file
#SBATCH --array=1-26749%1000  #ATB:1-59851%1000
#SBATCH --mail-type=BEGIN,END,FAIL

#======================================================
# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
#======================================================
RUN_SCRIPT=/nfs/research/martin/uniprot/research/spneumo_analysis/process_unifire_predictions.py

STR_ADD=up
#STR_ADD=atb

BASEDIR=/nfs/research/martin/uniprot/research/unifire_runs
FASTA_DIR=${BASEDIR}/work_${STR_ADD}
INFILE=${FASTA_DIR}/${STR_ADD}_unifire_result_paths.txt

OFFSET=0 # UP
#OFFSET=59850 #ATB
LINE_NUMBER=$((${SLURM_ARRAY_TASK_ID} + ${OFFSET}))
FASTA_FILE=$(sed -n ${LINE_NUMBER}p ${INFILE})

echo Starting Unifire result processing for: ${STR_ADD}
echo Reading from: ${FASTA_DIR}
echo No of lines: $(wc -l ${INFILE})

printf "Input file:${INFILE}\nTASK-ID: ${SLURM_ARRAY_TASK_ID}\nFASTA_FILE: ${FASTA_FILE}\nLINE_NUMBER: ${LINE_NUMBER}\n"

#echo time process_unifire_predictions.py unifire_paths.txt
time ${RUN_SCRIPT} ${FASTA_FILE} --force
