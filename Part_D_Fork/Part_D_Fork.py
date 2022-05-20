# -*- coding: utf-8 -*-
from mrjob.job import MRJob
import time

#constantinople fork late feb (2-3)
class Part_D_Fork(MRJob):
    
    def mapper(self, _, line):
        
        try:
            fields = line.split(",")
            if len(fields) == 7:
                date = time.gmtime(int(fields[6]))
                date_2 = (time.strftime("%Y", date),
                          time.strftime("%m", date),
                          time.strftime("%d", date))
                if date_2[0:2] == ("2019","02") or date_2[0:2] == ("2019", "03"):
                    gas_paid = int(fields[4])
                    gas_price = int(fields[5])
                    yield(date_2, (gas_paid, gas_price, 1))
                    
        except:
            pass

    def reducer(self, date, results):
        
        total_gas_paid = 0
        gas_price = 0
        amount_transactions = 0

        for i in results:
            total_gas_paid += i[0]
            gas_price += i[1]
            amount_transactions += i[2]

        average_gas_price = gas_price / amount_transactions
        yield(date, (total_gas_paid, average_gas_price, amount_transactions))

if __name__ == "__main__":
    Part_D_Fork.JOBCONF = {"mapreduce.job.reduces" : "3"}
    Part_D_Fork.run()