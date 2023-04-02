#!/usr/bin/python
import sys
import re
import os

""" MAP: 
Input:
- A customized inverted index [term_k, [URL_i@W_i]ord]
Output:
- A list of candidate pairs [URL_ij@W_i@W_j, 1]  

High level of what the first mapper will do:
MAP:    for each term, elements in input_data:
            get the information of given query object, 
            and derive its URLs, total term
            for the rest object sharing the same term:
                extract URL, total term
                emit candidate pair

"""
def read_input(file, separator='\t'):
    # get term_k\tURL_1@W_1\tURL_2@W_2
    for line in file:
        term, urls  = line.split(separator, 1)
        yield term, urls.split(separator) 

def transform(content):
    # lowercase
    content = content.lower()
    # remove punctualtions
    content = re.sub(r'[^\w\s]', '', content)
    # remove stop words

    # lemmatization
    return content

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    # get URL of query
    # file_url = os.getenv('mapreduce_map_input_file')
    # file_url = file_url if file_url else "random_filename"
    # get content of query from hadoop environment
    raw_query = os.getenv('q_from_user')
    query_words = set(transform(raw_query if raw_query else '').split())

    for term, elements in data:
        if term not in query_words:
            continue

        urlq, wq = 'query.txt', len(query_words)
        for element in elements:
            url, w = element.split('@')
            if url != urlq:
                if int(w) > int(wq):
                    print("{}-{}@{}@{}\t{}".format(url, urlq, int(w), int(wq), 1))
                else:
                    print("{}-{}@{}@{}\t{}".format(urlq, url, int(wq), int(w), 1))

if __name__ == "__main__":
    main()