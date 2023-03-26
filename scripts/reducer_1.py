#!/usr/bin/python3

from itertools import groupby
from operator import itemgetter
import sys
import os

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
    total_map = os.getenv('total_map_tasks')
    try:
        total_map = int(total_map.strip())
    except:
        total_map = 0
    for current_word, group in groupby(data, itemgetter(0)):
        uacs = [url_and_count for _, url_and_count in group]
        if len(uacs) == 1 or len(uacs) == total_map:
            # lonely word or common word
            continue
        sorted_uacs = sorted(read_mapper_output(uacs, '@'), key=itemgetter(1))
        sorted_uacs_formatted = ['@'.join(uac) for uac in sorted_uacs]
        print("%s\t%s" % (current_word, separator.join(sorted_uacs_formatted)))

if __name__ == "__main__":
    main()

