from mrjob.job import MRJob
import time

class Part_D_Gas_1(MRJob):
    
    def mapper(self, _, line):
        
        try:
            fields = line.split(",")
            if len(fields) == 7:
                date = time.gmtime(int(fields[6]))
                date_2 = (time.strftime("%Y", date), 
                          time.strftime("%m", date))
                gas = float(fields[5])
                yield(date_2, (gas,1))
                
        except:
            pass

    def reducer (self, date, gas):
        
        total_gas = 0
        count = 0

        for i in gas:
            total_gas += i[0]
            count += i[1]

        mean_gas = total_gas / count
        yield (date, mean_gas)

if __name__ == "__main__":
    Part_D_Gas_1.JOBCONF = {"mapreduce.job.reduces" : "3"}
    Part_D_Gas_1.run()
