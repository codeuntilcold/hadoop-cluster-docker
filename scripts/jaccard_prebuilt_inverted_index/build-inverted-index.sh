#!/bin/bash

# create input files
mkdir input 2> /dev/null

echo "The lazy dog got jumped over by the fox, also very lazy." > input/file1.txt
echo "The quick, brown fox jumps over the lazy dog." > input/file2.txt
echo "The cat is very good at jumping" > input/file3.txt
echo "Document needs to be interesting, but writer is lazy" > input/file4.txt

total_map=$(ls input | wc -l)
unique_idx=$(expr $(hdfs dfs -ls | wc -l) / 2)
inverted_index_dir="output-inverted-index"

echo "Total map is $total_map"
echo "Results will be output to $inverted_index_dir"

# create input directory on HDFS
hadoop fs -mkdir -p input 2> /dev/null

# put input files to HDFS
hdfs dfs -put ./input/* input 2> /dev/null

# run wordcount
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
        -Dmapreduce.reduce.env="total_map_tasks=$total_map" \
        -files /root/inverted_index_mapper.py,/root/inverted_index_reducer.py \
        -mapper inverted_index_mapper.py \
        -reducer inverted_index_reducer.py \
        -input input \
        -output $inverted_index_dir

# print the output of wordcount
echo -e "\nInverted Index Output"
hdfs dfs -cat $inverted_index_dir/part-*