import pyspark

sc = pyspark.SparkContext()

lines = sc.textFile("/data/bitcoin/vout.csv")

header = lines.first()
wikileaks_tx = lines.filter(lambda x: x != header)\
                    .map(lambda x: x.split(","))\
                    .filter( lambda x: '{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}' in x[3])\
                    .map(lambda x: (x[0], (x[1], x[2], x[3]))) # txhash (value, n (vout))

lines1 = sc.textFile("/data/bitcoin/vin.csv")
header2 = lines1.first()

join_vin = lines1.filter(lambda x: x != header2)\
                    .map(lambda x: x.split(","))\
                    .map(lambda x: (x[0], (x[1], x[2] ))) # txid (hash, vout)

wiki_vin_join = wikileaks_tx.join(join_vin)


donors_wiki = wiki_vin_join.map(lambda x: ("".join([x[1][1][0], x[1][1][1]]), float(x[1][0][0]))) #join(hash,vout)value

final_join = lines.filter(lambda x: x!=header)\
                    .map(lambda x: x.split(","))\
                    .map(lambda x: ("".join([x[0],x[2]]), x[3])) #join(txid, vout), pubkey

donators = donors_wiki.join(final_join)

top10_donators = donators.takeOrdered(10, key=lambda x: -x[1][0])

with open("output/top10_donators.txt", "w") as fp:
    fp.write('\n'.join('{} {} '.format(x[1][0],x[1][1]) for x in top10_donators))

#top10 = donators.takeOrdered(10, key=lambda x: x[1])
#
#output = sc.parallelize(donators.collect())
#
#for i in output.take(5):
#    print(i)

#output.saveAsTextFile("out")

##wikileaks_tx = wikileaks_tx.reduceByKey(lambda a,b: a+b)
#
#sc.parallelize(wikileaks_tx).saveAsTextFile("out")
#
#wiki_leaks = lines.filter(is_wiki_leaks).map(lambda l: l.split(','))
#vout_join = wiki_leaks.map(lambda l: (l[0],  (float(l[1], l[2]))))
##wiki_leaks has format (k = hash_val, v = (btc num))
#lines2 = sc.textFile("/data/bitcoin/vin.csv")
#
#is_clean_vin = lines.filter(clean_vin).map(lambda l: l.split(','))
#vin_join = is_clean_vin.map(lambda l: (l[1], (l[0], l[2])))
##vin_join has format (k = txid, v=tx_hash)
#
#vin_vout_join = vout_join.join(vin_join)
#
#vin_vout_join.saveAsTextFile("out")
#
#vin_vout_join.takeSample(True, 500)
#for i in vin_vout_join:
#    print(i)
