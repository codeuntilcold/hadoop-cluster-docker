#!/bin/bash

echo "Enter your query"
read query

# create input files
mkdir input
echo "The lazy dog got jumped over by the fox, also very lazy." > input/file1.txt
echo "The quick, brown fox jumps over the lazy dog." > input/file2.txt
echo "The cat is very good at jumping" > input/file3.txt
echo "Document needs to be interesting, but writer is lazy" > input/file4.txt

# count total map tasks
echo "Total map is $(ls input | wc -l)"

# create input directory on HDFS
hadoop fs -mkdir -p input

# put input files to HDFS
hdfs dfs -put ./input/* input

# run wordcount
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
        -Dmapreduce.map.env="q_from_user=$query" \
        -Dmapreduce.reduce.env="total_map_tasks=$(ls input | wc -l)" \
        -files /root/mapper_1.py,/root/reducer_1.py \
        -mapper mapper_1.py \
        -reducer reducer_1.py \
        -input input \
        -output "output-$(hdfs dfs -ls | wc -l)"

# print the output of wordcount
echo -e "\nfirst phase output"
hdfs dfs -cat output-$(expr $(hdfs dfs -ls | wc -l) - 1)/part-00000

