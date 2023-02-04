from mrjob.job import MRJob
from mrjob.step import MRStep
import math
import nltk
from nltk.tokenize import word_tokenize
import numpy as np


class health_tfidf(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_word_frequency,
                reducer=self.reducer_sum_word_frequency
                ),
            MRStep(mapper=self.mapper_move_to_values,
                reducer=self.reducer_sum__total_health_count
                ),
            MRStep(mapper=self.mapper_tfidf,
                reducer=self.reducer_display_tfidf
                )
        ]
    
    def mapper_word_frequency(self,_,line):
        #splitting the preprocessed text file data by :
        line_arr = line.split(':')
        #first half of split are the twitter ids of each post assigned to tid
        tid = int(line_arr[0]) 
        #second half of the split is the post text which is assigned to words
        words = line_arr[1]
        #storing the totla number of words in each twitter text
        word_count = len(words.split(" "))
        #iterate through a for loop to get all documents which contain 'health'
        for t in words:
            if "health" in words:
                #yield by twitter id and each posts word count
                yield tid, word_count
                
    def reducer_sum_word_frequency(self,key,values):
        #make values into a list
        values = list(values)
        #word count to the 1st element of the list of values
        word_count = values[0]
        # we are doing an estimation due to not being able to sucessfully determining how to calculate health in each document
        health_count = 1 
        #yielding the key which is the twitter id and values of health count and word count together in a list
        yield key, (health_count, word_count)
        
    def mapper_move_to_values(self,tid,counts):
        #break counts into two variables of health count and word count
        health_count, word_count = counts
        #yield all data into the values part for the next reducer
        yield "health", (tid, health_count, word_count)
     
    def reducer_sum__total_health_count(self, key, values):
        #make our values into a list
        values = list(values)
        #provide the total word size and assign to our total_health_count
        total_health_count = len(values)
        #iterate through to yield all past values and now the total health count to store to each document
        for _id, health_count, word_count in values:
            yield _id, (health_count, word_count, total_health_count)

    def mapper_tfidf( self, tid, counts):
        # break apart counts variable to get all value variables from past reducer
        health_count, word_count, total_health_count = counts
        #we know that the total amount of documents equal to 10000
        total_documents = 10000
        #we will now use these variables to calculate the tf and idf score seperately then times both scores
        #tf score formula
        tf_score = health_count/word_count
        #idf formula
        idf_score = math.log10(total_documents/total_health_count)
        #tfidf scores
        tf_idf = tf_score * idf_score
        #yield to get the all twitter ids of documents containing 'health' and their tfidf scores they get for 'health'
        yield tid, tf_idf
    
    def reducer_display_tfidf(self,key,value):
        #display the results
        #next() iterates to each next value
        yield key, next(value)
        
if __name__ == "__main__":
    health_tfidf.run()