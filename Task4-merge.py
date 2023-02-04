from mrjob.job import MRJob
# from mrjob.runner import MrJobRunner
import re

#sort values at the end of reducer
MRJob.SORT_VALUES = True

WORD_RE = re.compile(r"[\w']+")
class mergeSortids(MRJob):
    #split words by line
    def mapper(self,_, line):
        for tid in WORD_RE.findall(line):
            #the id with an id assigned and value 1
            yield tid,1

    def reducer(self, key, value):
        #display the id and the sum of each id 
        #converted the id to an integer
        yield int(key), sum(value)

if __name__ == '__main__':
    mergeSortids.run()