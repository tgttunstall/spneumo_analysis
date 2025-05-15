#!/bin/env python

import argparse

def group_consecutive(nums):
    if not nums:
        return []

    nums = sorted(set(nums))
    result = []
    start = end = nums[0]

    for n in nums[1:]:
        if n == end + 1:
            end = n
        else:
            result.append(f"{start}-{end}" if start != end else f"{start}")
            start = end = n

    result.append(f"{start}-{end}" if start != end else f"{start}")
    return result

def main():
    parser = argparse.ArgumentParser(description="Group consecutive numbers into ranges.")
    parser.add_argument("--input", "-i", required=True, help="Path to input file containing numbers (one per line)")
    parser.add_argument("--output", "-o", required=False, help="Path to output file to write grouped ranges (optional). If not provided, prints to screen")

    args = parser.parse_args()

    # Read numbers from input file
    with open(args.input, "r") as infile:
        nums = [int(line.strip()) for line in infile if line.strip().isdigit()]

    # Group numbers and write to output file
    grouped = group_consecutive(nums)
    output_str = ",".join(grouped)

    if args.output:
        with open(args.output, "w") as outfile:
            outfile.write(output_str)
        print(f"Grouped ranges written to {args.output}")
    else:
        # Print to screen with standout formatting
        print("\n" + "="*40)
        print("Grouped Ranges:")
        print(output_str)
        print("="*40 + "\n")

if __name__ == "__main__":
    main()

