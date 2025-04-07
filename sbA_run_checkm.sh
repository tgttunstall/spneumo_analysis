#!/bin/bash
  
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=168:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=20  # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=128G   # memory per node
#SBATCH -J "CheckM_ATB_array_1k_run2"   # job name
#SBATCH -o "console_checkm/CheckM_ATB_%A_%a.out"   # job output file
#SBATCH --array=1-60000:1000%60 #1-60000:500%60
#SBATCH --mail-type=BEGIN,END,FAIL,ARRAY_TASKS

#======================================================
# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
#======================================================
#source /hps/software/users/jlees/tanushree/checkm//bin/activate
INDIR='/nfs/research/martin/uniprot/research/checkm_runs/input_atb'

# run1: 1:60000
#INFILE=${INDIR}/input_list_atb_checkm_1.txt
#OUTDIR=/nfs/research/martin/uniprot/research/checkm_runs/output_atb/run1/$SLURM_ARRAY_TASK_ID

# run2: 60001-119701
INFILE=${INDIR}/input_list_atb_checkm_2.txt
OUTDIR=/nfs/research/martin/uniprot/research/checkm_runs/output_atb/run2/$SLURM_ARRAY_TASK_ID

INPUT_LIST_FILE=$INDIR/temp-$SLURM_ARRAY_TASK_ID
#INPUT_LIST_FILE=$(mktemp)

mkdir -pv ${OUTDIR}
cm_threads=${SLURM_CPUS_PER_TASK}
pp_threads=${SLURM_CPUS_PER_TASK}

# Print for logging

# this chunk size must match the number after the colon in the --array in the 
# preamble, as it is the number of lines that will be processed from $INFILE.
#JOB_ARRAY_CHUNKSIZE=500
JOB_ARRAY_CHUNKSIZE=1000

# Job Array skip -1
LAST_LINE=$((${SLURM_ARRAY_TASK_ID}+$JOB_ARRAY_CHUNKSIZE-1))
echo "First line to process: ${SLURM_ARRAY_TASK_ID}. Last line to process: $LAST_LINE"
sed -n "${SLURM_ARRAY_TASK_ID},${LAST_LINE}p" ${INFILE} > $INPUT_LIST_FILE

# Create a temp input list file for CheckM to use


#checkm lineage_wf /nfs/research/martin/uniprot/research/checkm_data/input_list_cm_pedro_v2.txt /nfs/research/martin/uniprot/research/checkm_data/test_checkm/output/pedro_v2 -f checkm_results_pedro_v2.tsv --genes -x fasta -t 11 --pplacer_threads 11

# Run CheckM on this chunk
checkm lineage_wf "$INPUT_LIST_FILE" "${OUTDIR}" \
  -f "${OUTDIR}/chunk_${SLURM_ARRAY_TASK_ID}.tsv" \
  --tab_table \
  --genes \
  -x faa \
  -t ${cm_threads} \
  --pplacer_threads ${pp_threads}

#rm ${INPUT_LIST_FILE}

