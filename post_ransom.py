import pyspark
from datetime import datetime

sc = pyspark.SparkContext()

line1 = sc.textFile("/data/bitcoin/vout.csv")
head1 = line1.first()

ransom_wallet = line1.filter( lambda x: x != head1 )\
                        .map( lambda x: x.split(","))\
                        .filter( lambda x: "{1AEoiHY23fbBn8QiJ5y6oAjrhRY1Fb85uc}" in x[3])\
                        .map( lambda x: (x[0],  (x[1], x[3])))

line2 = sc.textFile("/data/bitcoin/transactions.csv")
head2 = line2.first()

outgoing_tx = line2.filter( lambda x: x != head2 )\
                    .map( lambda x: x.split(","))\
                    .map( lambda x: (x[0], (x[2], x[4])))\
                    .join(ransom_wallet)\
                    .map( lambda x: (datetime.fromtimestamp(int(x[1][0][0])).strftime("%Y-%m"), float(x[1][1][0]),
                                     x[1][1][1], x[1][0][1]))
                    

aa = outgoing_tx.collect()
sc.parallelize(aa).saveAsTextFile("out")

#outgoing_tx.saveAsTextFile("out")
                    


#line2 = sc.textFile("/data/bitcoin/transactions.csv")
#head2 = line2.first()
#
#post_ransom_tx = line2.filter( lambda x: x != head2)\
#                        .map( lambda x: x.split(","))\
#                        .map( lambda x: (x[0], (x[2], x[4])))\
#                        .join( ransom_wallet)\
#                        .map( lambda x: (x[0], (datetime.fromtimestamp(int(x[1][0][0])).strftime("%Y-%m"), x[1][0][1],
#                                              x[1][1][0], x[1][1][1])))

