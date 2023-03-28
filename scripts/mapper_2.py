#!/usr/bin/python
import sys

""" MAP-2: 
Input:
- A customized inverted index [term_k, [URL_i@W_i]ord]
Output:
- A list of candidate pairs [URL_ij@W_i@W_j, 1]  

High level of what the first mapper will do:
MAP-2:  for each term, elements in input_data:
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

def get_query_object(doc_list):
    for doc in doc_list:
        url, w = doc.split('@')
        if url == 'D3': # query.txt
            return url, w
    
    return None, None

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    for term, elements in data:
        urlq, wq = get_query_object(elements)
        if urlq is not None:
            for element in elements:
                url, w = element.split('@')
                if url != urlq:
                    if int(w) > int(wq):
                        print("{}-{}@{}@{}\t{}".format(url, urlq, int(w), int(wq), 1))
                    else:
                        print("{}-{}@{}@{}\t{}".format(urlq, url, int(wq), int(w), 1))

if __name__ == "__main__":
    main()