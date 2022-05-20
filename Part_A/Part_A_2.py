# -*- coding: utf-8 -*-
from mrjob.job import MRJob
import time

class Part_A_2(MRJob):

    def mapper(self, _, line):
        
        try:
            fields = line.split(",")
            if len(fields) == 7:
                date = time.gmtime(int(fields[6]))
                value_transaciton = int(fields[3])
                date_2 = (time.strftime("%Y", date), 
                         time.strftime("%m", date))
                yield (date_2, (value_transaciton, 1))
                
        except:
            pass

    def reducer(self, date, value):
        
        total_value = 0
        count = 0

        for i in value:
            total_value += i[0]
            count += i[1]

        mean_value = total_value / count
        yield (date, mean_value)

if __name__ == "__main__":
    Part_A_2.JOBCONF = {"mapreduce.job.reduces" : "3"}
    Part_A_2.run()
