#!/bin/bash 
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=48:0:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=48   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=128G   # memory per node
#SBATCH -J "checkM2_UP"   # job name
#SBATCH --output=console_checkm2/CheckM2_UP_%A_%a.out # job output file
#SBATCH --array=1-27%27   #tested for atb with 1%1, and ran 1-120%60
#SBATCH --mail-type=BEGIN,FAIL,END,REQUEUE,INVALID_DEPEND,ARRAY_TASKS

#mamba activate checkm2

INDIR=/nfs/research/martin/uniprot/research/spneumo_dataset/up_data_1k/${SLURM_ARRAY_TASK_ID}
OUTDIR=/nfs/research/martin/uniprot/research/checkm2_runs/output_up_1k/${SLURM_ARRAY_TASK_ID}
EXT=fa
mkdir -pv ${OUTDIR}

checkm2 predict \
  --genes \
  -x ${EXT} \
  --threads ${SLURM_CPUS_PER_TASK} \
  --input ${INDIR}/ \
  --output-directory ${OUTDIR} \
  --force
