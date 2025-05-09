#!/bin/bash
  
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=48:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=11  # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=64G   # memory per node
#SBATCH -J "ATB_add_taxid"  # job name UP: "UP_add_taxid"
#SBATCH -o "logs_taxid/ATB_add_taxid_%A_%a.out"   # job output file: "logs_taxid/UP_add_taxid.."
#SBATCH --array=1-59851%1000  # atb data round 2

#======================================================
# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
#    SBATCH --mail-type=BEGIN,END,FAIL  #ARRAY_TASKS
#======================================================
# Array notes
#1-59850%1000 (ATB round 1 with offset=0) #1-59851%1000 (ATB round 2 with offset=59850) #1-119701 (ATB total lines/files) #1-26749%1000 (UP data) #1-5%2 (Test) 

# PURPOSE OF SCRIPT:
# add taxid i.e the string 'OX=1313' to the headers of fasta files in order to make unifire run as this is a mandatory field

#STR_ADD=up
STR_ADD=atb

#INDIR=/nfs/research/martin/uniprot/research/unifire_test
#INFILE=${INDIR}/input_list_all.txt

INDIR=/nfs/research/martin/uniprot/research/spneumo_dataset
#INFILE=${INDIR}/up_proteome_paths_TT.txt
INFILE=${INDIR}/${STR_ADD}_proteome_paths_TT.txt

#INPUT_LIST_FILE=${INFILE}/temp-${SLURM_ARRAY_TASK_ID}
#INPUT_LIST_FILE=$(mktemp)

#OFFSET=0 #UP data and ATB data (round 1)
OFFSET=59850 # ATB round 2i.e when runnig lines 59851-119701 i.e --array=1-59850
LINE_NUMBER=$((${SLURM_ARRAY_TASK_ID} + ${OFFSET}))
FASTA_FILE=$(sed -n ${LINE_NUMBER}p ${INFILE})

echo Input file: ${FASTA_FILE}
echo Total number of proteins: $(grep -c '>' ${FASTA_FILE})
echo
time sed -Ei 's/(>.*)$/\1 OX=1313/g' ${FASTA_FILE}
echo Total number of lines with added taxid: $(grep -c 'OX=1313' ${FASTA_FILE})

#time sed -Ei 's/OX=TAXID//g' ${FASTA_FILE}
#time sed -Ei 's/  //g' ${FASTA_FILE}
