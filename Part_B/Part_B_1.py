# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class Part_B_1(MRJob):
    
    def mapper(self, _, line):
        
        try:
            fields = line.split(",")
            if len(fields) == 7:
                address = fields[2]
                value = int(fields[3])
                yield(address, value)
                
        except:
            pass

    def reducer(self, address, value):
        yield(address, sum(value))

if __name__ == "__main__":
    Part_B_1.JOBCONF = {"mapreduce.job.reduces" : "3"}
    Part_B_1.run()
