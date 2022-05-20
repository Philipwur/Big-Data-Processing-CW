# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class Part_B_3(MRJob):

    def mapper(self, _, line):

        fields = line.split("\t")
        address = fields[0][1:-1]
        value = int(fields[1])
        yield(None, (address, value))

    def reducer(self, _, contract):

        sorted_values = sorted(contract, reverse = True, key = lambda x: x[1])
        
        for i in sorted_values[:10]:
             yield(i[0], i[1])

if __name__ == "__main__":
    Part_B_3.JOBCONF = {"mapreduce.job.reduces" : "3"}
    Part_B_3.run()
