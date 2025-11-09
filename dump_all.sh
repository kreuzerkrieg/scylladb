#!/bin/bash

# Set working directory
cd /home/ernest.zaslavsky/Development/scylladb/foobarbazing

# Output file for all extracted values
output_file="all_values.txt"
> "$output_file"  # Clear the file if it exists

# Collect all TOC files into a single argument list
sstables=(./*-TOC.txt)

# Dump all SSTables at once to a temporary file
tmp_dump="tmp_dump.json"
/home/ernest.zaslavsky/Development/scylladb/build/release/scylla sstable dump-data \
  --sstables "${sstables[@]}" \
  --output-format json > "$tmp_dump"

# Extract values and append to output file
jq -r '.sstables[][] | .key.value' "$tmp_dump" >> "$output_file"

jq '[.sstables[][] | .clustering_elements | length] | add' "$tmp_dump" > clustering_count.txt

