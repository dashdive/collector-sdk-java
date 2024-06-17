#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <filename> <number_of_times>"
    exit 1
fi

filename=$1
num_times=$2

rm -f "$filename"

for (( i=1; i<=num_times; i++ )) do
    echo "abcdefghijklmnopqrstuvwxyz" >> "$filename"
done
