#!/usr/bin/env python

import argparse
import logging
from pathlib import Path
import sys
import pandas as pd

PREDICTION_FILES = {
    'arba': 'predictions_arba.out',
    'unirule': 'predictions_unirule.out',
    'pirsr': 'predictions_unirule-pirsr.out',
}

def setup_logging(loglevel, logfile):
    logger = logging.getLogger("proteome_logger")
    logger.setLevel(getattr(logging, loglevel))
    
    # File handler for missing files
    fh = logging.FileHandler(logfile) #<-- Use user-specified log file
    #fh = logging.FileHandler("missing_files_log.txt")
    #fh.setLevel(logging.WARNING)
    fh.setLevel(getattr(logging, loglevel))  # <-- Use user-specified log level
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # Console handler for info/warnings/errors
    ch = logging.StreamHandler()
    #ch.setLevel(getattr(logging, loglevel))
    ch.setLevel(getattr(logging, loglevel))  # <-- Use user-specified log level
    ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    
    logger.handlers = []  # Clear any existing handlers
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def process_proteome_dir(proteome_path: Path, force: bool, logger: logging.Logger):
    """
    Combine prediction files in a given proteome directory into a single output file.
    Logs missing files.
    """
    dfs = []
    missing_files = []
    for source, filename in PREDICTION_FILES.items():
        file_path = proteome_path / filename
        if file_path.exists():
            try:
                df = pd.read_csv(file_path, sep='\t')
                df['source'] = source
                dfs.append(df)
            except Exception as e:
                logger.warning(f"{proteome_path}: Failed to read {filename}: {e}")
        else:
            missing_files.append(filename)
            logger.warning(f"{proteome_path}: {filename} not found.")

    if missing_files:
        logger.warning(f"{proteome_path}: Missing files: {' '.join(missing_files)}")

    if not dfs:
        logger.info(f"{proteome_path}: No prediction files found, skipping.")
        return

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df['proteome_id'] = proteome_path.name

    output_file = proteome_path / f"all_predictions_{proteome_path.name}.out"
    if output_file.exists() and not force:
        logger.error(f"{output_file} already exists. Use --force to overwrite.")
        return
    elif output_file.exists() and force:
        logger.info(f"Overwriting existing file {output_file}")

    try:
        combined_df.to_csv(output_file, sep='\t', index=False)
        logger.info(f"Written: {output_file}")
    except Exception as e:
        logger.error(f"Failed to write {output_file}: {e}")

def process_input(input_path: Path, force: bool, logger: logging.Logger):
    """
    Detects input type and processes accordingly:
    - Single directory with predictions*out files
    - Directory of subdirectories (each with predictions*out files)
    - File containing list of directories
    """
    processed = 0
    skipped = 0

    if input_path.is_file():
        # Treat as file containing list of directories
        logger.info(f"Processing list of directories from file: {input_path}")
        with open(input_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                proteome_dir = Path(line)
                if proteome_dir.is_dir():
                    process_proteome_dir(proteome_dir, force, logger)
                    processed += 1
                else:
                    logger.warning(f"{proteome_dir} is not a valid directory, skipping.")
                    skipped += 1
    elif input_path.is_dir():
        # Check for predictions*out files directly in this directory
        has_predictions = any((input_path / fname).exists() for fname in PREDICTION_FILES.values())
        if has_predictions:
            logger.info(f"Processing single proteome directory: {input_path}")
            process_proteome_dir(input_path, force, logger)
            processed += 1
        else:
            # Check for subdirectories
            subdirs = [d for d in input_path.iterdir() if d.is_dir()]
            if subdirs:
                logger.info(f"Processing {len(subdirs)} subdirectories in {input_path}")
                for subdir in subdirs:
                    process_proteome_dir(subdir, force, logger)
                    processed += 1
            else:
                logger.error(f"No prediction files or subdirectories found in {input_path}. Nothing to do.")
                skipped += 1
    else:
        logger.error(f"Input {input_path} is neither a file nor a directory.")
        sys.exit(1)

    logger.info(f"Processing complete. {processed} processed, {skipped} skipped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine prediction files in proteome directories into a single output file. "
                    "Supports a single directory, a directory of subdirectories, or a file with directory paths."
    )
    parser.add_argument('--input', required=True, help='Path to a proteome directory, a directory of subdirectories, or a file containing directory paths.')
    parser.add_argument('--force', action='store_true', help='Force overwrite of existing output files.')
    parser.add_argument('--log', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set logging level (default: INFO)')
    parser.add_argument('--logfile', default='missing_files_log.txt', help='Filename for logging output (default: missing_files_log.txt)')

    args = parser.parse_args()
    
    input_path = Path(args.input)
    
    if not input_path.exists():
        print(f"Error: {input_path} does not exist.")
        sys.exit(1)

    logger = setup_logging(args.log, args.logfile)
    process_input(input_path, args.force, logger)

# Usage
# Default logging (INFO)
#python script.py --input /path/to/proteome_dir

# More verbose logging
#python script.py --input /path/to/proteome_dir --log DEBUG

# Only warnings and errors
#python script.py --input /path/to/proteome_dir --log WARNING

# Force overwrite
#python script.py --input /path/to/proteome_dir --force

