from mrjob.job import MRJob

class fuck(MRJob):

    def mapper(self, _, line):
        x =  line.split(",")

        try:
            if '{1M1ZkWpHfuQthji2AVsGDvpfY2PrcXQar6}' in x[3]:
                yield "MTGOX hack" , x[1]
            if '{1L2JsXHPMYuAa9ugvHGLwkdstCPUDemNCf}' in x[3]:
                yield "BitStamp", x[1]
            if '{1LqrT5xMAgP1HjMKGruo97Kx8z4DndWCMM}' in x[3]:
                yield "MTGOX addr", x[1]
            else:
                yield "XXX", 1
        except: 
            self.increment_counter('custom', 'shouldnt be 0', 1)

#    def combiner(self, pubkey, btc):
#        yield pubkey, sum(btc)

    def reducer(self, pubkey, btc):
        yield pubkey, sum(btc)

if __name__ == '__main__':
    fuck.run()

