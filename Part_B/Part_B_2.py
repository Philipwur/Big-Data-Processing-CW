# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class Part_B_2(MRJob):

    def mapper(self, _, line):
        
            if len(line.split(",")) == 5:

                fields = line.split(",")
                address = fields[0]
                yield (address, (None, 1))


            elif len(line.split("\t")) == 2:

                fields = line.split("\t")
                address = fields[0][1:-1]
                value = int(fields[1])
                yield (address, (value, 2))

    def reducer(self, address, values):

        flag = False
        value = 0

        for i in values:
            if i[1] == 1:
                flag = True
            elif i[1] == 2:
                value += int(i[0])

        if flag and value != 0:
            yield (address, value)

if __name__ == "__main__":

	Part_B_2.JOBCONF = {"mapreduce.job.reduces" : "3"}
	Part_B_2.run()
