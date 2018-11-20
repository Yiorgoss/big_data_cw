import pyspark

sc = pyspark.SparkContext()

def is_wiki_leaks(line):
    try:
        arr = line.split(',')
        if len(arr) != 4:
            return False

        if arr[3] != '1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v':
            return False

        return True

    except:
        return True

lines = sc.textFile("/data/bitcoin/v_out.csv")

wiki_leaks = lines.filter(is_wiki_leaks)

features = wiki_leaks.map(lambda l: int((l.split(',')[1]), l.split(',')[2]))

