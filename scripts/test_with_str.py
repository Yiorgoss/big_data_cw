# coding: utf-8
yy = sc.parallelize([("tx_id", (( "btc", "vout", "pubkey"),("hash", "vout") ))])
ww = sc.parallelize([("hash", "btc", "vout", "pubkey")])
xx = yy.map(lambda a: ("".join([a[1][1][0],a[1][1][1]]), a[1][0][2]))
ww1 = ww.map(lambda x: ("".join([x[0], x[2]]), x[1], x[3]))
uu = ww1.join(xx)
