# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class Part_C_2(MRJob):

    def mapper(self, _, line):
        
        try:
            if len(line.split("\t")) == 2:
                fields = line.split("\t")
                miner = fields[0][1:-1]
                size = int(fields[1])
                yield(None, (miner, size))
                
        except:
            pass

    def reducer(self, _, result):
        
        sorted_values = sorted(result, reverse = True, key = lambda x: x[1])
        
        for i in sorted_values[:10]:
             yield(i[0], i[1])

if __name__ == "__main__":
    Part_C_2.JOBCONF = {"mapreduce.job.reduces" : "3"}
    Part_C_2.run()
