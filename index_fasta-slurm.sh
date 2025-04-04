#!/bin/bash
  
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=48:0:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=32   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=50G   # memory per node
#SBATCH -J "spneumo_atb_up_index"   # job name
#SBATCH --output /nfs/research/martin/uniprot/research/spneumo_dataset/slurm_logs/mmseq-slurm-%j.out
#SBATCH --mail-type=BEGIN,FAIL,END,REQUEUE,INVALID_DEPEND,ARRAY_TASKS


#======================================================
# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
#======================================================

source /nfs/research/martin/uniprot/research/spneumo_analysis/spneumo_mm/bin/activate

# Script: index_fasta_slurm.sh
# Description: Indexes a combined FASTA file using ffdb's indexer.py script.
#              Submits the indexing task to SLURM
#------------------------------------------------------------------------------

# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error.
set -u


# ------------------------------------------------------------------------------
# CONSTANTS and DEFAULTS
# ------------------------------------------------------------------------------

BASEDIR="/nfs/research/martin/uniprot/research/spneumo_dataset"
DEFAULT_OUTPUT_DIR="$BASEDIR/combined_data"
#DEFAULT_OUTPUT_DIR="$BASEDIR/test_ds/data"
DEFAULT_PREFIX="spneumo"  # Default prefix for combined FASTA and index files
INDEXER_SCRIPT="/nfs/research/martin/uniprot/research/spneumo_analysis/spneumo_mm/bin/indexer.py" # Update this path

# Resource parameters:
THREADS=32
MEM="50"
TIME="48-0"

# ------------------------------------------------------------------------------
# FUNCTIONS
# ------------------------------------------------------------------------------

usage() {
    echo "Usage: $0 -f <combined_fasta> [-o <output_dir>] [-p <prefix>] [-h]"
    echo "  -h    Show this help message"
    echo "  -f    Combined FASTA file to be indexed (required)"
    echo "  -o    Output directory for index files (default: $DEFAULT_OUTPUT_DIR)"
    echo "  -p    Prefix for index files (default: $DEFAULT_PREFIX)"
}

# ------------------------------------------------------------------------------
# ARGUMENT PARSING
# ------------------------------------------------------------------------------

combined_fasta=""
output_dir="$DEFAULT_OUTPUT_DIR"
prefix="$DEFAULT_PREFIX"

while getopts ":hf:o:p:" opt; do
    case $opt in
        h)
            usage
            exit 0
            ;;
        f)
            combined_fasta="$OPTARG"
            ;;
        o)
            output_dir="$OPTARG"
            ;;
        p)
            prefix="$OPTARG"
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            usage >&2
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            usage >&2
            exit 1
            ;;
    esac
done

>&2 echo " .-- BEGUN $(date) --."

# Check if the combined FASTA file is provided and exists
if [ -z "$combined_fasta" ]; then
    echo "Error: No combined FASTA file specified. Use -f to specify the file." >&2
    usage >&2
    exit 1
fi

if [ ! -f "$combined_fasta" ]; then
    echo "Error: Specified FASTA file does not exist: $combined_fasta" >&2
    exit 1
fi

# ------------------------------------------------------------------------------
# MAIN WORKFLOW
# ------------------------------------------------------------------------------

mkdir -pv "$output_dir"

index_filename=$(basename $combined_fasta)
outfile_index="$output_dir/$index_filename.idx"

# Indexing command:

# Unsorted: $INDEXER_SCRIPT -v -u -e '>' -i '(\w+[\|\.]*\w+).*$' -r -f $combined_fasta >$outfile_index
$INDEXER_SCRIPT -v -t $THREADS -e '>' -i '(\w+[\|\.]*\w+).*$' -r -f $combined_fasta >$outfile_index

>&2 echo " .-- END $(date) --."
