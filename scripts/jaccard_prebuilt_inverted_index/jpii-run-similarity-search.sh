#!/bin/bash

echo "Enter your query"
read query

output_dir="output-$(hdfs dfs -ls | wc -l)"
# run similarity search
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
        -Dmapreduce.map.env="q_from_user=$query" \
        -files /root/jpii_mapper.py,/root/jpii_reducer.py,  \
        -mapper jpii_mapper.py \
        -reducer jpii_reducer.py \
        -input output-inverted-index \
        -output $output_dir

# print the output of wordcount
echo -e "\nSimilairty Result"
hdfs dfs -cat $output_dir/part-*