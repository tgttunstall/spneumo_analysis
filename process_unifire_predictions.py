#!/usr/bin/env python

import pandas as pd
from pathlib import Path
import sys
import argparse

def process_proteome_dir(proteome_path: Path, force: bool):
    """
    Combine prediction files in a given proteome directory into a single output file.

    Parameters:
        proteome_path (Path): Path to the proteome directory.
        force (bool): If True, overwrite existing output file. If False, exit with a warning if output exists.

    The function looks for the following files in the directory:
        - predictions_arba.out
        - predictions_unirule.out
        - predictions_unirule-pirsr.out

    Each found file is read and combined into a single DataFrame, with an added 'source' column indicating its origin.
    The combined data is written to 'all_predictions_<proteome_dir>.out' in the same directory.
    """
    prediction_files = {
        'arba': 'predictions_arba.out',
        'unirule': 'predictions_unirule.out',
        'pirsr': 'predictions_unirule-pirsr.out',
    }

    dfs = []
    for source, filename in prediction_files.items():
        file_path = proteome_path / filename
        if file_path.exists():
            df = pd.read_csv(file_path, sep='\t')
            df['source'] = source
            dfs.append(df)
        else:
            print(f"Warning: {file_path} not found, skipping...")

    if not dfs:
        print(f"No prediction files found in {proteome_path}, skipping.")
        return

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df['proteome_id'] = proteome_path.name

    output_file = proteome_path / f"all_predictions_{proteome_path.name}.out"

    # Check for existing file and handle force flag
    if output_file.exists() and not force:
        print(f"Error: {output_file} already exists. Use --force to overwrite.")
        sys.exit(1)
    elif output_file.exists() and force:
        print(f"Warning: Overwriting existing file {output_file}")

    combined_df.to_csv(output_file, sep='\t', index=False)
    print(f"Written: {output_file}")

def process_input_path(input_path: Path, force: bool):
    """
    Process either a single proteome directory or a file listing multiple directory paths.

    Parameters:
        input_path (Path): Path to a directory or a file containing directory paths (one per line).
        force (bool): If True, overwrite existing output files. If False, exit with a warning if output exists.

    For a file input, each non-empty line is treated as a directory path to process.
    """
    if input_path.is_file():
        print(f"Processing file list: {input_path}")
        with open(input_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                proteome_dir = Path(line)
                if proteome_dir.is_dir():
                    process_proteome_dir(proteome_dir, force)
                else:
                    print(f"Warning: {proteome_dir} is not a valid directory, skipping...")
    elif input_path.is_dir():
        process_proteome_dir(input_path, force)
    else:
        print(f"Error: {input_path} is neither a valid file nor directory.")
        sys.exit(1)

if __name__ == "__main__":
    """
    Script entry point.

    Usage:
        python process_proteome_dir.py <proteome_dir_path OR file_paths.txt> [--force]

    Arguments:
        proteome_dir_path: Path to a single proteome directory.
        file_paths.txt: File containing multiple directory paths (one per line).

    Options:
        --force: Overwrite existing output files if they exist.
    """
    parser = argparse.ArgumentParser(
        description='Combine prediction files in proteome directories into a single output file, with optional overwrite.'
    )
    parser.add_argument('input', help='Proteome directory path or file containing multiple paths')
    parser.add_argument('--force', action='store_true',
                      help='Force overwrite of existing output files')
    args = parser.parse_args()

    input_path = Path(args.input)

    if not input_path.exists():
        print(f"Error: {input_path} does not exist.")
        sys.exit(1)

    process_input_path(input_path, args.force)


# Usage:
# Safe mode (no overwrite)
#process_unifire_predictions.py /path/to/proteome_dir

# Force overwrite
#process_unifire_predictions.py /path/to/proteome_dir --force

# Process multiple directories with force
#process_unifire_predictions.py file_list.txt --force

