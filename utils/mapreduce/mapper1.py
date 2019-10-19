#!/usr/bin/env python

import sys
import os

stopwords= ['a','able','about','across','after','all','almost','also','am','among','an','and','any','are','as','at','be','because','been','but','by',
            'can','cannot','could','dear','did','do','does','either','else','ever','every','for','from','get','got','had','has','have','he','her','hers',
            'him','his','how','however','i','if','in','into','is','it','its','just','least','let','like','likely','may','me','might','most','must','my',
            'neither','no','nor','not','of','off','often','on','only','or','other','our','own','rather','said','say','says','she','should','since','so',
            'some','than','that','the','their','them','then','there','these','they','this','tis','to','too','twas','us','wants','was','we','were','what',
            'when','where','which','while','who','whom','why','will','with','would','yet','you','your'];


# input comes from STDIN (standard input)
for line in sys.stdin:
    doc = os.environ["map_input_file"]
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    for word in words:
        word=word.lower();
        #removing any punctuation / non alphanumeric character
        wordc = ''.join(ch for ch in word if ch.isalnum())
        if (wordc!='' and wordc not in stopwords):
            word_doc=wordc+' '+doc;
            # writing results to STDOUT (standard output)
            print '%s\t%s' % (word_doc, 1)


        
        
