#!/bin/bash

echo "Enter your query"
read query

# create input files
# mkdir input 2> /dev/null
# echo "The lazy dog got jumped over by the fox, also very lazy." > input/file1.txt
# echo "The quick, brown fox jumps over the lazy dog." > input/file2.txt
# echo "The cat is very good at jumping" > input/file3.txt
# echo "Document needs to be interesting, but writer is lazy" > input/file4.txt
 
# input_phase1_dir="input"
# total_map=$(ls input | wc -l)

input_phase1_dir="gutenberg"

echo $query > $input_phase1_dir/query.txt

# copy data to hdfs
hdfs dfs -rm -r $input_phase1_dir
hdfs dfs -mkdir -p $input_phase1_dir
hdfs dfs -put $input_phase1_dir/* $input_phase1_dir

total_map=$(expr $(hdfs dfs -ls $input_phase1_dir | wc -l) - 2)
unique_idx=$(expr $(hdfs dfs -ls -d output-phase1* | wc -l))
output_phase1_dir="output-phase1-$unique_idx"
output_phase2_dir="output-phase2-$unique_idx"

# count total map tasks
echo "Total map is $total_map"
echo "Results will be output to $output_phase1_dir, $output_phase2_dir"

hdfs dfs -rm $input_phase1_dir/query.txt
hdfs dfs -put $input_phase1_dir/query.txt $input_phase1_dir/query.txt

# run phase 1
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
	-Dmapreduce.task.timeout=0 \
        -Dmapreduce.map.env="q_from_user=$query" \
	-Dmapreduce.map.memory.mb=2048 \
        -Dmapreduce.reduce.env="total_map_tasks=$total_map" \
	-Dmapreduce.reduce.memory.mb=4096 \
        -files /root/mapper_1.py,/root/reducer_1.py \
        -mapper mapper_1.py \
        -reducer reducer_1.py \
        -input $input_phase1_dir \
        -output $output_phase1_dir 

# return code for phase1
return_phase1=$?

echo -e "\nfirst phase output -- printing $output_phase1_dir"
hdfs dfs -cat $output_phase1_dir/part*

if [ $return_phase1 != 0 ]; then
	echo "terminating phase 2 since phase 1 did not succeed"
	exit 1
fi

# run phase 2
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
        -files /root/mapper_2.py,/root/reducer_2.py \
        -mapper mapper_2.py \
        -reducer reducer_2.py \
        -input $output_phase1_dir \
        -output $output_phase2_dir

echo -e "\nsecond phase output -- printing $output_phase2_dir"
hdfs dfs -cat $output_phase2_dir/part*

