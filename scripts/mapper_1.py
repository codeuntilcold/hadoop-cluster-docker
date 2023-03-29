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
    for line in file.split('\n'):
        yield transform(line)

def main(separator='\t', second_sep='@'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    file_url = os.getenv('mapreduce_map_input_file')
    file_url = file_url if file_url else "random_filename"
    if '/' in file_url:
        # take the name of the document as the url -- doc1.txt, doc2.txt, query.txt,...
        file_url = file_url.split('/')[-1]

    document_words = set()
    for line in data:
        document_words.update(line.split())
    raw_query = os.getenv('q_from_user')
    query_words = set(transform(raw_query if raw_query else '').split())
    intersection = document_words.intersection(query_words)

    for word in intersection:
        print('{}{}{}{}{}'.format(word, separator, file_url, second_sep, len(document_words)))

if __name__ == "__main__":
    main()

