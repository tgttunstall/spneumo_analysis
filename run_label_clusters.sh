#!/bin/bash
#source /nfs/research/martin/uniprot/research/spneumo_analysis/spneumo_mm/bin/activate

sp_name=spneumo
logsdir=logs_${sp_name}
threads=8 #for align_fastafiles
#minproteomes=1

#add_name=atb
add_name=up
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
fastadir=${BASEDIR}/spneumo_all # UP+ATB and also UP
#fastadir=${BASEDIR}/atb_data # ATB ONLY

results_dir="${output_dir}/results_${setname}_c${COVERAGE}_p${MIN_SEQ_ID}_cm${COV_MODE}_s${SEQ_ID_MODE}_l${CLUSTER_MODE}"
results_labelled_file=${results_dir}/Labelled_Species_protein_cluster.tsv                                                                                          

batchsize=4000 #for label_clusters
mem=30G #for label_clusters

# Ensure logs directory exists
mkdir -pv "$logsdir"
echo "Reached here..."
# ------------------------------------------------------------------------------
# MAIN WORKFLOW: Submit SLURM job
# ------------------------------------------------------------------------------
#out_file ${results_dir}/Labelled_Species_protein_cluster.tsv \
#  -J "label ${sp_name} clusters parallel t${TASKS} b${batchsize}" \ 
sbatch \
  -J "label ${sp_name} clusters parallel t${TASKS} b${batchsize} ${add_name}" \
  -o "${logsdir}/labelling_c${COVERAGE}_p${MIN_SEQ_ID}_cm${COV_MODE}_s${SEQ_ID_MODE}_l${CLUSTER_MODE}.out" \
  --parsable \
  --mem="${mem}" \
  --time=1-0 \
  --ntasks=1 \
  --cpus-per-task="${TASKS}" \
  --mail-type=FAIL,END,REQUEUE,INVALID_DEPEND,ARRAY_TASKS \
  --mail-user="${USER}" \
  --wrap="./label_clusters_gi.py \
    --fasta_dir ${fastadir}/ \
    --input_file ${results_dir}/Species_protein_cluster.tsv \
    --out_file ${results_labelled_file} \
    --extension .fa \
    -t ${TASKS} \
    --batchsize ${batchsize} \
    --sortlabels \
    --uniq"

# ------------------------------------------------------------------------------
# Data for cluster-sizes: Submit SLURM job
# ------------------------------------------------------------------------------
#FIXME: create this var at the beginning as a global and also pass thsi to the sbatch for labelling
#results_clustersize_file=$results_dir}/Count_Labelled_Species_protein_cluster.tsv
#cut -f 1,3 input_file | sort -u | cut -f 1 | sort | uniq -c | awk '{print $1}' | sort -n > output_file
#cut -f 1,3 ${results_labelled_file} | sort -u | cut -f 1 | sort | uniq -c | awk '{print $1}' | sort -n > ${results_clustersize_file}

