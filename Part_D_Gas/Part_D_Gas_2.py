from mrjob.job import MRJob
import time

class Part_D_Gas_2(MRJob):
    
    def mapper(self, _, line):
        
        try:

            fields = line.split(",")
            if len(fields) == 9:
                date = time.gmtime(int(fields[7]))
                date_2 = (time.strftime("%Y", date), 
                        time.strftime("%m", date))
                size = int(fields[5])
                gas_used = int(fields[6])
                yield(date_2, (size, gas_used, 1))
                
        except:
            pass

    def reducer (self, date, results):
        
        total_size = 0
        total_gas_used = 0
        count = 0

        for i in results:
            total_size += i[0]
            total_gas_used += i[1]
            count += i[2]

        mean_size = total_size / count
        mean_gas_used = total_gas_used / count

        yield (date, (mean_size, mean_gas_used))

if __name__ == "__main__":
    Part_D_Gas_2.JOBCONF = {"mapreduce.job.reduces" : "8"}
    Part_D_Gas_2.run()
