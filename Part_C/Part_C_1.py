# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class Part_C_1(MRJob):

    def mapper(self, _, line):
        
        try:
            if len(line.split(",")) == 9:
                fields = line.split(",")
                miner = fields[2]
                size = int(fields[4])
                yield(miner, size)
                
        except:
            pass

    def reducer(self, miner, size):
        yield(miner, sum(size))

if __name__ == "__main__":
    Part_C_1.JOBCONF = {"mapreduce.job.reduces" : "3"}
    Part_C_1.run()
