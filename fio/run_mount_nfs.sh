#!/bin/bash

# Set up variables
#JOB_NAME="benchmark"
#OUTPUT_DIR="/path/to/output/directory"

BLOCK_SIZES=("4k" "512k")
IO_DEPTHS=("1" "16" "32")
NFS_VERSS=("3" "4")

# Set NFS URL from first positional parameter
NFS_URL="$1"

# Check if NFS URL parameter is present
if [ -z "$NFS_URL" ]
then
    echo "NFS URL parameter is required."
    exit 1
fi

# Iterate through block sizes and IO depths
for block_size in "${BLOCK_SIZES[@]}"
do
    for io_depth in "${IO_DEPTHS[@]}"
    do
        # Set up output file name
        output_file="${OUTPUT_DIR}/${JOB_NAME}_bs${block_size}_iodepth${io_depth}.json"

        #--output-format=json

        # Run the benchmark
        fio --bs=$block_size --iodepth=$io_depth --runtime=$RUN_TIME libnfs.fio  --output=$output_file 
    done
done