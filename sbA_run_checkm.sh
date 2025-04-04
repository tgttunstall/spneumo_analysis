#!/bin/bash
  
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=168:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=11  # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=128G   # memory per node
#SBATCH -J "CheckM_ATB"   # job name
#SBATCH -o "console_checkm/CheckM_ATB_%A_%a.out"   # job output file
#SBATCH --array=1-6:2%2
#SBATCH --mail-type=BEGIN,END,FAIL,ARRAY_TASKS

#======================================================
# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
#======================================================
#source /hps/software/users/jlees/tanushree/checkm//bin/activate
#INDIR='/nfs/research/martin/uniprot/research/checkm_runs'
#INFILE=${INDIR}/${HEAD_input_list_atb_checkm.txt}
#OUTDIR=${INDIR}/output_atb
#CHUNK_SIZE=2  # Number of files per chunk/job

#mkdir -pv ${OUTDIR}
#cm_threads=${SLURM_CPUS_PER_TASK}
#pp_threads=11

# Print for logging
echo "Task ID: ${SLURM_ARRAY_TASK_ID}"

# Create a temp input list file for CheckM to use


#checkm lineage_wf /nfs/research/martin/uniprot/research/checkm_data/input_list_cm_pedro_v2.txt /nfs/research/martin/uniprot/research/checkm_data/test_checkm/output/pedro_v2 -f checkm_results_pedro_v2.tsv --genes -x fasta -t 11 --pplacer_threads 11

# Run CheckM on this chunk
#checkm lineage_wf "$INPUT_LIST_FILE" "${OUTDIR}" \
#  -f "${OUTDIR}/chunk_${SLURM_ARRAY_TASK_ID}.tsv" \
#  --genes \
#  -x fa \
#  -t ${cm_threads} \
#  --pplacer_threads ${pp_threads}

