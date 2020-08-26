from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import matplotlib.pyplot as plt

def load_wordlist(filename):
	words = []
	f = open(filename, 'r')
	text = f.read()
	text = text.split('\n')
	for line in text:
		words.append(line)
	f.close()
	return words

def updateFunction(newValues, runningCount):
	if runningCount is None:
		runningCount = 0
	return sum(newValues, runningCount) 


def stream(ssc, pwords, nwords, duration):
	start = int(0)
	topic = 'test'
	partition = 0    
	topicPartion = TopicAndPartition(topic,partition)
	fromOffset = {topicPartion: start}
	kstream = KafkaUtils.createDirectStream(ssc, ['test'], {"metadata.broker.list": 'localhost:9092'}, fromOffset)
	tweets = kstream.map(lambda x: x[1])
	words = tweets.flatMap(lambda line:line.split(' '))
	positive = words.map(lambda word: ('Positive', 1) if word.lower() in pwords else ('Positive', 0))
	negative = words.map(lambda word: ('Negative', 1) if word.lower() in nwords else ('Negative', 0))
	allSentiments = positive.union(negative)
	sentimentCounts = allSentiments.reduceByKey(lambda x,y: x+y)
	#runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
	#runningSentimentCounts.pprint()
	counts = []
	sentimentCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
	sentimentCounts.pprint()
	ssc.start() 
	ssc.awaitTerminationOrTimeout(duration)
	ssc.stop(stopGraceFully = True)
	return counts

def make_plot(counts):
	positiveCounts = []
	negativeCounts = []
	time = []
	for val in counts:
		try:
			positiveTuple = val[0]
			positiveCounts.append(positiveTuple[1])
		except:
			positiveCounts.append(0)
		try:
			negativeTuple = val[1]
			negativeCounts.append(negativeTuple[1])
		except:
			negativeCounts.append(0)
	for i in range(len(counts)):
		time.append(i)
	posLine = plt.plot(time, positiveCounts,'bo-', label='Positive')
	negLine = plt.plot(time, negativeCounts,'go-', label='Negative')
	plt.axis([0, len(counts), 0, max(max(positiveCounts),max(negativeCounts))+50])
	plt.xlabel('Time step')
	plt.ylabel('Word count')
	plt.legend(loc = 'upper left')
	plt.show()

if __name__ == '__main__':
	conf = SparkConf().setAppName('PythonStreamingKafkaTweetCount')
	sc = SparkContext(conf = conf)
	ssc = StreamingContext(sc, 10)
	ssc.checkpoint("checkpoint")
	pwords = load_wordlist("positive.txt")
	nwords = load_wordlist("negative.txt")
	counts = stream(ssc, pwords, nwords, 0)
	print(counts)
	make_plot(counts)
    

