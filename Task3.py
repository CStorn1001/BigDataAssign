from mrjob.job import MRJob
import re
#re.compile is reverse compiling
#store variable searches for alphanumerics or apostrophes which are 1 or longer. 
WORD_RE = re.compile(r"[\w']+")
class NumOccurWord(MRJob):
    #split words by line
    def mapper(self,_, line):
        for city in WORD_RE.findall(line):
            #yields each city and asigns a 1 to them
            yield city,1
    def reducer(self, city, counts):
        #combines all cities that are the same to get the total number of tweets for each city
        yield city, sum(counts)
if __name__ == '__main__':
    NumOccurWord.run()