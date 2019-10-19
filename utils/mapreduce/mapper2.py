#!/usr/bin/env python

import sys


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    word_doc,count=line.split('\t',1)
    word,doc=word_doc.split(' ',1)
    word_count=word+' '+count;
    # write the results to STDOUT (standard output);
    print '%s\t%s' % (doc, word_count)
        
