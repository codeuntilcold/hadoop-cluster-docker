#!/bin/bash

echo "Enter your query"
read query

# create input files
mkdir input 2> /dev/null
echo "The lazy dog got jumped over by the fox, also very lazy." > input/file1.txt
echo "The quick, brown fox jumps over the lazy dog." > input/file2.txt
echo "The cat is very good at jumping" > input/file3.txt
echo "Document needs to be interesting, but writer is lazy" > input/file4.txt

echo $query > input/query.txt

total_map=$(ls input | wc -l)
unique_idx=$(expr $(hdfs dfs -ls | wc -l) / 2)
output_phase1_dir="output-phase1-$unique_idx"
output_phase2_dir="output-phase2-$unique_idx"

# count total map tasks
echo "Total map is $total_map"
echo "Results will be output to $output_phase1_dir, $output_phase2_dir"

# copy data to hdfs
hadoop fs -mkdir -p input 2> /dev/null
hdfs dfs -put ./input/* input 2> /dev/null

# run phase 1
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
        -Dmapreduce.map.env="q_from_user=$query" \
        -Dmapreduce.reduce.env="total_map_tasks=$total_map" \
        -files /root/mapper_1.py,/root/reducer_1.py \
        -mapper mapper_1.py \
        -reducer reducer_1.py \
        -input input \
        -output $output_phase1_dir 

echo -e "\nfirst phase output -- printing $output_phase1_dir"
hdfs dfs -cat $output_phase1_dir/part*

# run phase 2
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
        -files /root/mapper_2.py,/root/reducer_2.py \
        -mapper mapper_2.py \
        -reducer reducer_2.py \
        -input $output_phase1_dir \
        -output $output_phase2_dir

echo -e "\nsecond phase output -- printing $output_phase2_dir"
hdfs dfs -cat $output_phase2_dir/part*

