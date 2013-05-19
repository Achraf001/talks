import sys
from celery import Celery, chain, group
from collections import defaultdict
import re

celery = Celery('variance',
                broker='redis://localhost/1',
                backend='redis')

@celery.task
def extract_words(filename):
    words = []
    with open(filename) as fh:
        for line in fh:
            words.extend(re.split('\W+', line))
    return words
    

@celery.task
def ngrams(words, n):
    res = []
    for i in range(len(words) - n):
        res.append(tuple(words[i : i + n]))
    return res


@celery.task
def frequency(words):
    freq = {}
    for word in words:
        if word not in freq:
            freq[word] = 0
        freq[word] += 1
    return freq

if __name__ == '__main__':
    files = sys.argv[1:]

    

    wf = group( chain ( extract_words.s(filename) | ngrams.s(2) | frequency.s() ) for filename in files )
    print wf
    print wf().get()

