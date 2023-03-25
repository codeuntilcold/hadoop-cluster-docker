#!/usr/bin/python3

import sys
import re
import os

"""
High level of what the first mapper will do
MAP1:   [D_i]   -->     [term_k, URL_i@W_i]
        for each line in D_i
            extract term_k
        get number of terms of a document W_i
        get urls of documents
        filter terms that are in the query
        emit term_k, URL_i@W_i
"""

def transform(content):
    # lowercase
    content = content.lower()
    # remove punctualtions
    content = re.sub(r'[^\w\s]', '', content)
    # remove stop words

    # lemmatization
    return content

def read_input(file):
    file = file.read()
    file = transform(file)
    for line in file.split('\n'):
        yield line

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    file_url = os.getenv('mapreduce_map_input_file')
    words = set()
    for line in data:
        for word in line.split():
            words.add(word)

    raw_query = os.getenv('q_from_user')
    query = set(transform(raw_query if raw_query else '').split())
    if not query.issubset(words):
        return
    for word in words:
        print('%s\t%s@%d' % (word, file_url if file_url else "insert_random_file_name_pls", len(words)))

if __name__ == "__main__":
    main()

