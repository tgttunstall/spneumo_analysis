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
    parser.add_argument("--output", "-o", required=True, help="Path to output file to write grouped ranges")

    args = parser.parse_args()

    # Read numbers from input file
    with open(args.input, "r") as infile:
        nums = [int(line.strip()) for line in infile if line.strip().isdigit()]

    # Group numbers and write to output file
    grouped = group_consecutive(nums)
    with open(args.output, "w") as outfile:
        outfile.write(",".join(grouped))

if __name__ == "__main__":
    main()

