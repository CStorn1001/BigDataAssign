from mrjob.job import MRJob
import re
#re.compile is reverse compiling
#store variable searches for alphanumerics or apostrophes which are 1 or longer. 
WORD_RE = re.compile(r"[\w']+")
class NumOccurWord(MRJob):
    #split words by line
    #(_, line) tells MRJob to ignore the key and take each line of the document as the value.
    def mapper(self,_, line):
        #iterate through each line of text and print each word and assign the value 1 
        for word in WORD_RE.findall(line):
            yield word,1
    def reducer(self, word, counts):
        #display each word and combine all words that are the same and sum up values (number of occurances)
        yield word, sum(counts)
if __name__ == '__main__':
    NumOccurWord.run()
