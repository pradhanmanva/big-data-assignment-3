from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second.
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1) # 1 - batchDuration 

# Create a DStream that will connect to hostname:port, like localhost:9999
# It represents the stream of data that will be received from the data server. Each record in this DStream is a line of text.
lines = ssc.socketTextStream("localhost", 9999)

# split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# count each word in each batch
pairs = words.map(lambda word: (word, 1))

wordCounts = pairs.reduceByKey(lambda x, y: x+ y)

# print the first ten elements of each RDD generated in this DStream to the console

wordCounts.pprint()

# Note that when these lines are executed, Spark Streaming only sets up the computation it will perform when it is started, 
# and no real processing has started yet. 
# To start the processing after all the transformation have been setup, we finally call

ssc.start() # start the computation
ssc.awaitTermination() # Wait for the computation to terminate


