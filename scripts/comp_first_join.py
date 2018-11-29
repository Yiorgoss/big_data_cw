# coding: utf-8
aa = sc.textFile("/data/bitcoin/vout.csv")
bb = sc.textFile("/data/bitcoin/vin.csv")
header = aa.first()
header1 =  bb.first()
cc = aa.filter(lambda x: x != header).map(lambda x: x.split(",")).filter(lambda x: '{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}' in x[3]).map(lambda x: (x[0], (x[1], x[2], x[3])))
dd = bb.filter(lambda x: x != header1).map(lambda x: x.split(",")).map(lambda x: (x[0], (x[1],x[2])))
ee = cc.join(dd)
ee.persist()
