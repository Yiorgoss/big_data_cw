from mrjob.job import MRJob

from datetime import datetime

class timeAnalysis(MRJob):

    #Create a bar plot showing the number of transactions which occurred every month between the start and end of the dataset. What do you notice about the overall trend in the utilisation of bitcoin?Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.
    def mapper(self, _, line):
        arr = line.split(",")
        try: 
            date = datetime.fromtimestamp( int(arr[2])).strftime("%Y-%m")
            yield date, 1
        except:
            self.increment_counter('custom', 'bad_lines', 1)

    def combiner( self, date, count):
        yield date, sum(count)

    def reducer(self, date, count):
        yield date, sum(count)

if __name__ == '__main__':
        timeAnalysis.run()
