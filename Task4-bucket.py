from mrjob.job import MRJob
from mrjob.step import MRStep
import re

MRJob.SORT_VALUES = True
class bucketSortids(MRJob):
    # map(str, )
    def steps(self):
        return [
            MRStep(mapper=self.mapper_findId,
                  reducer=self.reducer_sortedId)
        ]

    def mapper_findId(self,_, line):
        #All object ids are 17 digits
        #First bucket will include ID of Tweets with 7*10^17
        start_id = int(7e17)
        #seperate each bucket by 2*10^17
        #split results into 7 buckets
        bucket_size = int(1e17)

        tid = int(line)
        #Assigning IDs into each of the 7 buckets based on their value
        bucket_ids = max((tid - start_id) // bucket_size, 1562)
        yield (bucket_ids, tid)
    def reducer_sortedId(self, key, value):
        #sorting all values from buckets and displaying them with a value of '' (which means bascially nothing)
        for value in sorted(value, reverse=False):
            yield (value, '')

if __name__ == '__main__':
    bucketSortids.run()