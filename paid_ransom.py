import pyspark
from datetime import datetime 

sc = pyspark.SparkContext()

line1 = sc.textFile( "/data/bitcoin/vout.csv")
header1 = line1.first()

ransom_wallet = line1.filter( lambda x: x != header1)\
                        .map( lambda x: x.split(","))\
                        .filter( lambda x: '{1AEoiHY23fbBn8QiJ5y6oAjrhRY1Fb85uc}' in x[3])\
                        .map( lambda x: ((x[0]), (x[1])))

line2 = sc.textFile("/data/bitcoin/vin.csv")
header2 = line2.first()

ransom_tx = line2.filter( lambda x: x != header2)\
                    .map( lambda x: x.split(","))\
                    .map( lambda x: ((x[0]), (x[1])))\
                    .join(ransom_wallet)\
                    .map( lambda x: (x[1][0][0], (x[1][1][0])))

line3 = sc.textFile("/data/bitcoin/transactions.csv")
header3 = line3.first()

date_paid_ransom = line3.filter( lambda x: x != header3)\
                        .map( lambda x: x.split(","))\
                        .map( lambda x: (x[0], (datetime.fromtimestamp( int(x[2])).strftime("%Y-%m"))))\
                        .join(ransom_tx)\
                        .map(lambda x: (x[1][0], (float(x[1][1]))))\
                        .reduceByKey(lambda x,y: x+y)

date_paid_ransom.saveAsTextFile("out")




                    

