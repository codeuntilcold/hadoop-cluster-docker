#!/usr/bin/python3

from itertools import groupby
from operator import itemgetter
import sys

"""
High level of what the first reducer will do
REDUCE1:    [term_k, URL_i@W_i] --> [term_k, [URL_i@W_i] ordered]
            for each term, group when group by term:
                if group length != total number of documents
                    and group length != 1 
                    then emit term, sorted group
"""

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    for current_word, group in groupby(data, itemgetter(0)):
        uacs = [url_and_count for _, url_and_count in group]
        sorted_uacs = sorted(read_mapper_output(uacs, '@'), key=itemgetter(1))
        sorted_uacs_formatted = ['@'.join(uac) for uac in sorted_uacs]
        print("%s\t%s" % (current_word, separator.join(sorted_uacs_formatted)))

if __name__ == "__main__":
    main()

