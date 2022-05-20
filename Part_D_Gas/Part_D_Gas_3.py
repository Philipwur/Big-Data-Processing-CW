from mrjob.job import MRJob
import time

class Part_D_Gas_3(MRJob):
    
    def mapper(self, _, line):
        
        try:
            fields = line.split(",")
            if len(fields) == 7:
                address = fields[2]
                date = time.gmtime(int(fields[6]))
                gas = int(fields[4])
                date_2 = (time.strftime("%Y", date), 
                          time.strftime("%m", date))
                #this is the address of the most valuable smart contract
                if address == "0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444":
                    yield(date_2, (gas, 1))

        except:
            pass

    def reducer(self, date, gas):

        total_gas = 0
        month_transactions = 0

        for i in gas:
            total_gas += i[0]
            month_transactions += i[1]

        mean_gas = total_gas / month_transactions
        yield (date, (month_transactions, total_gas, mean_gas))

if __name__ == "__main__":
    Part_D_Gas_3.JOBCONF = {"mapreduce.job.reduces" : "10"}
    Part_D_Gas_3.run()
