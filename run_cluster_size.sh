#!/bin/bash

#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=10:0:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=32   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=50G   # memory per node
#SBATCH -J "mmseq_clustersize_calc_atb"   # job name
#SBATCH --output /slurm_-%j.out
#SBATCH --mail-type=BEGIN,FAIL,END,REQUEUE,INVALID_DEPEND,ARRAY_TASKS

source /nfs/research/martin/uniprot/research/spneumo_analysis/spneumo_mm/bin/activate

sp_name=spneumo
logsdir=logs_${sp_name}
threads=8 #for align_fastafiles
#minproteomes=1

add_name=atb
echo ${add_name}
# ------------------------------------------------------------------------------
# IMPORTED DIRECTLY FROM : easylinclust_tt.py
# ------------------------------------------------------------------------------
# imported from easylinclust_tt.py
BASEDIR="/nfs/research/martin/uniprot/research/spneumo_dataset"

# UP+ATB
#output_dir=${BASEDIR}/mmseqs_results
#combined_fasta=${BASEDIR}/combined_data/${sp_name}_combined.faa

#ATB only
output_dir=${BASEDIR}/mmseqs_results_${add_name}
combined_fasta=${BASEDIR}/combined_data/${sp_name}_combined_${add_name}.faa

setname=$(basename "$combined_fasta" | cut -d. -f1) # omits file extension

# Clustering parameters:
COV_MODE=1
SEQ_ID_MODE=0
MIN_SEQ_ID=0.90
COVERAGE=0.90
CLUSTER_MODE=2

TASKS=32 #for all distributed processing steps
# ------------------------------------------------------------------------------
# PARAMS for this script: Labelling 
# ------------------------------------------------------------------------------
#fastadir=${BASEDIR}/spneumo_all # UP+ATB
fastadir=${BASEDIR}/atb_data # ATB ONLY

results_dir="${output_dir}/results_${setname}_c${COVERAGE}_p${MIN_SEQ_ID}_cm${COV_MODE}_s${SEQ_ID_MODE}_l${CLUSTER_MODE}"
results_labelled_file=${results_dir}/Labelled_Species_protein_cluster.tsv                                                                                          

batchsize=4000 #for label_clusters
mem=30G #for label_clusters

# Ensure logs directory exists
mkdir -pv "$logsdir"

# ------------------------------------------------------------------------------
# Data for cluster-sizes: Submit SLURM job
# ------------------------------------------------------------------------------
results_clustersize_file=$results_dir}/Count_Labelled_Species_protein_cluster.tsv

#cut -f 1,3 input_file | sort -u | cut -f 1 | sort | uniq -c | awk '{print $1}' | sort -n > output_file
#echo cut -f 1,3 ${results_labelled_file} | sort -u | cut -f 1 | sort | uniq -c | awk '{print $1}' | sort -n > ${results_clustersize_file}
echo "BEGIN calculating clustersize output..."
cut -f 1,3 ${results_labelled_file} | sort -u | cut -f 1 | sort | uniq -c | awk '{print $1}' | sort -n > ${results_clustersize_file}
echo "END: clustersize output file: ${results_clustersize_file}"

echo "Counting lines in Labelled file: $(wc -l ${results_labelled_file})"
echo "Counting lines in Clustersize file: $(wc -l ${results_clustersize_file})"

