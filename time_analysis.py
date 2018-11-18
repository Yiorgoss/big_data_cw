from mrjob.job import MRJob
from mrjob.step import MRStep

from datetime import datetime

class timeAnalysis(MRJob):

    #Create a bar plot showing the number of transactions which occurred every month between the start and end of the dataset. What do you notice about the overall trend in the utilisation of bitcoin?Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.

    tx_vout = {}

    def steps(MRJob):
        return [MRStep(mapper_init=self.init_dict), 
                MRStep(mapper_init=self.join_tx_vin,
                        mapper=self.map_to_month_amount,
                        reducer=self.count_tx_amount),
                MRStep( reducer=self.split_month)
                ]

    
    def join_tx_vin_vout(self, _, line):
        arr = line.split(",")
        #vout.csv has format hash, value, n(vout from vin), publicKey
        

    def join_tx_vin(self, _, line):
        arr = line.split(",")
        #vin.csv has format txid, tx_hash, vout
        self.tx_vout[arr[1]].append(arr[2])

    def init_dict(self):
        with open("input/transactions.csv") as f:
           for line in f:
                arr = line.split(",")
                #line format = tx_hash, blockhash, time, tx_in_count, tx_out_count
                self.tx_vout[arr[0]] = [(datetime.datetime.fromtimestamp(arr[2]).strftime("%Y-%m"))]

if __name__ == '__main__':
        timeAnalysis.run()
