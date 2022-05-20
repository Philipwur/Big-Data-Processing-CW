# -*- coding: utf-8 -*-
from mrjob.job import MRJob
import time

class Part_A_1(MRJob):

    def mapper(self, _, line):
        
        try:
            fields = line.split(",")
            if len(fields) == 7:
                date = time.gmtime(int(fields[6]))
                date_2 = (time.strftime("%Y", date), 
                          time.strftime("%m", date))
                yield(date_2, 1)
                
        except:
            pass

    def reducer(self, month, counts):
        yield(month, sum(counts))

if __name__ == "__main__":
    Part_A_1.JOBCONF = {"mapreduce.job.reduces" : "3"}
    Part_A_1.run()
