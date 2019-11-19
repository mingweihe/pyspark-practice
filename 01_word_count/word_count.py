from pyspark import SparkContext

# Terminal command: spark-summit word_count.py
if __name__ == '__main__':
    # appname: word count, number of cpu scores: 3
    sc = SparkContext('local[3]', 'word count') 
    sc.setLogLevel('ERROR')
    # read file and generate RDD object, a general way of RDD is sc.parallelize(['a', 'c', 'b'])
    lines = sc.textFile('./rawdata.txt')
    print(lines.count())
    # fisrt is a kind of rdd action
    print(lines.first())
    # filter is a kind of rdd transformation
    print(lines.filter(lambda x: x.strip()).count())
    print(lines.filter(lambda x: x.strip() != '').count())
    # type is PipelinedRDD, this is a subclass of RDD
    words = lines.flatMap(lambda line: line.split())
    # total number of word
    print(words.count())
    # count words by value, and to collections.defaultdict
    wordCounts = words.countByValue()
    # sort the result by value
    wordCounts = sorted(wordCounts.items(), key=lambda x: -x[1])
    # print the top 20 records
    for word, count in wordCounts[:20]:
        print(f'{word}: {count}')
