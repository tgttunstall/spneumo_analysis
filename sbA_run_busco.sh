#!/bin/bash
  
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=168:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=8  # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=64G   # memory per node
#SBATCH -J "BUSCO_ATB"   # job name
#SBATCH -o "console_busco/BUSCO_ATB_%A_%a.out"   # job output file
#SBATCH --array=1-59851%1000
#SBATCH --mail-type=BEGIN,END,FAIL,ARRAY_TASKS

#======================================================
# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
#======================================================
#source /hps/software/users/martin/uniprot/src/anaconda3/bin/activate busco5

LINEAGE_DB="/nfs/production/martin/uniprot/production/proteomes/busco5/busco_downloads/lineages/lactobacillales_odb10"
IN_DIR="/nfs/research/martin/uniprot/research/spneumo_dataset"
INFILE=${IN_DIR}/atb_proteome_paths_TT.txt
#FASTA_FILE=$(sed -n ${SLURM_ARRAY_TASK_ID}p ${INFILE}) # used it when --array=1-59850%1000
#echo "Printing from ${INFILE}, TASK-ID: ${SLURM_ARRAY_TASK_ID}, FASTA_FILE: ${FASTA_FILE}"

OFFSET=59850
LINE_NUMBER=$((${SLURM_ARRAY_TASK_ID} + ${OFFSET}))
FASTA_FILE=$(sed -n ${LINE_NUMBER}p ${INFILE})

echo "Printing from ${INFILE}, TASK-ID: ${SLURM_ARRAY_TASK_ID}, FASTA_FILE: ${FASTA_FILE} LINE_NUMBER: ${LINE_NUMBER}"

echo "Starting BUSCO"
echo "BUSCO executing from $(which busco)"

ATB_PROTEOME=$(basename $FASTA_FILE)
file_suffix=$(basename $FASTA_FILE .faa)
out_file="${file_suffix}_out.faa"

echo "LINEAGE: ${LINEAGE_DB}"
echo "ATB_PROTEOME: ${ATB_PROTEOME}"
echo "${file_suffix}"
echo "OUTFILE: ${out_file}"

#echo time busco -i /nfs/research/martin/uniprot/research/spneumo_dataset/atb_data/SAMEA5226197.faa -o SAMEA5226197_out.faa -l /nfs/production/martin/uniprot/production/proteomes/busco5/busco_downloads/lineages/lactobacillales_odb10 -m protein -f -c 3 --offline

#SLURM_CPUS_PER_TASK=10
# Run BUSCO on the FASTA file with a prefix for the output files
echo -n "Running BUSCO for ${ATB_PROTEOME} with the following options:
        busco -i ${FASTA_FILE}
  -o ${out_file}
  -l ${LINEAGE_DB}  
  -m protein
  -f
  -c ${SLURM_CPUS_PER_TASK}
  -- offline"

busco -i ${FASTA_FILE} \
  -o ${out_file} \
  -l ${LINEAGE_DB} \
  -m protein \
  -f \
  -c ${SLURM_CPUS_PER_TASK} \
  --offline


echo
start_time=$(date +%s)
echo "Start Time: $(date -d @$start_time +'%Y-%m-%d %H:%M:%S')"
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

# Convert elapsed time to hours, minutes, seconds
hours=$(($elapsed_time / 3600))
minutes=$(($elapsed_time % 3600 / 60))
seconds=$(($elapsed_time % 60))

echo "Start Time: $(date -d @$start_time +'%Y-%m-%d %H:%M:%S')"
#echo "End Time: $(date -d @$end_time +'%Y-%m-%d %H:%M:%S')"
echo "Processed Time: $hours hour(s) $minutes minute(s) $seconds second(s)"
#echo "Finished BUSCO: $(date -d @$end_time +'%Y-%m-%d %H:%M:%S')"
