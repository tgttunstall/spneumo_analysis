#!/bin/bash

# Define base and input directories
INDIR="/nfs/research/martin/uniprot/research/busco_runs"
OUTDIR="/nfs/research/martin/uniprot/research/busco_output"

mkdir -pv "${OUTDIR}"

# Output file path
OUTFILE_BUSCO="${OUTDIR}/busco_results_atb.tsv"

# Export output path so it's available in sub-shells
export OUTFILE_BUSCO

# Header for the TSV file with new column names
echo -e "biosample\tcomplete_combined_score\tcomplete_single_score\tcomplete_duplicate_score\tfragment_score\tmissing_score\tn_markers\tdomain" > "${OUTFILE_BUSCO}"

echo Concatenating BUSCO results files for ATB from: "${INDIR}"

# Find all JSON files and process them
time find "${INDIR}" -name 'short_summary.specific.*.json' -exec bash -c '
  file="$1"
  OUTFILE_BUSCO="$2"
#  biosample=$(basename "${file}" .json)  # Extract biosample from filename

  biosample=$(jq -r ".parameters.out" "${file}" | sed -e "s/_out.faa$//" -e "s/.*\.\([^\.]*\)$/\1/")
  # Use jq to extract required fields, reorder them with biosample first, and format them as TSV
  jq -r --arg biosample "${biosample}" \
    '\''[ $biosample, .results.Complete, .results."Single copy", .results."Multi copy", .results.Fragmented, .results.Missing, .results.n_markers, .results.domain ] | @tsv'\'' \
    "${file}" >> "${OUTFILE_BUSCO}"                                                                                                                           
' _ {} "${OUTFILE_BUSCO}" \;

echo 
echo "Concatenated file written to: ${OUTFILE_BUSCO}"
echo "No. of lines in file: $(wc -l < "${OUTFILE_BUSCO}")"

###################################
# Alternate to export, using arg passing:
# Find all JSON files and process them
#find /path/to/fasta/directories -name '*.json' -exec bash -c '
#  file="$0"
#  output="$1"
#  biosample=$(basename "${file}" .json)  # Extract biosample from filename
#
#  # Use jq to extract required fields, reorder them with biosample first, and format them as TSV
#  jq -r --arg biosample "$biosample" \
#    '"'"'[$biosample, .results.Complete, .results."Single copy", .results."Multi copy", .results.Fragmented, .results.Missing, .results.n_markers, .results.domain] | @tsv'"'"' \
#    "$file" >> "$output"
#' {} "$output" \;
#
