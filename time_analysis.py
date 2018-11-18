from mrjob.job import MRJob
from mrjob.step import MRStep

from datetime import datetime

class timeAnalysis(MRJob):

    #Create a bar plot showing the number of transactions which occurred every month between the start and end of the dataset. What do you notice about the overall trend in the utilisation of bitcoin?Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.

    tx_vin_vout = {}

    def steps(MRJob):
        return [ MRStep(mapper_init=self.join_tx_vin,
                        mapper=self.map_to_month_amount,
                        reducer=self.count_tx_amount),
                MRStep( reducer=self.split_month)
                ]

    def join_tx_vout_vin(self):
        with open("transactions.csv") as f:
           for line in f:
                arr = line.split(",")



    def mapper(self, _, line):
        arr = line.split(",")

        try:
            tx_hash = arr[0]
            date    = datetime.datetime.fromtimestamp(arr[2])
            

        except:
            self.increment_counter('custom','bad_lines', 1)



if __name__ == '__main__':
        timeAnalysis.run()
