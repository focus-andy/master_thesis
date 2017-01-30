from __future__ import print_function

import sys

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer

import rpeak
import json

def mapper( line ):
# unpack json
	raw_dict = json.loads( line )
	patient_id = raw_dict['p']
	ecg_data = {}
	for item in raw_dict['s']:
		if item['meta']['t'] == 'N2-ECG-1':
			ecg_data = item
			break
	algo = rpeak.RPeakDetector( fs=ecg_data['meta']['rate'], ecg_lead='MLI')
	peaks, rri = algo.analyze( ecg_data['data'] )
	output_data = {
		'peaks':[],
		'rri':[],
	}
	output_data.update( ecg_data )
	output_pack = json.dumps( output_data )
	
	return ( patient_id, ecg_data['meta']['n'] )

def toKafka( record ):
	k = "spark-" + str(record[0])
	v = json.dumps(record[1])
	print(k)
	print(v)

	conf = {
		'bootstrap_servers':'Niu-Kafka-0:9092',
		'key_serializer':str.encode,
#		'value_serializer':lambda v:v.encode('utf-8')
		'value_serializer':str.encode
	}
	producer = KafkaProducer( **conf )
	topic = 'stream-output'
	producer.send(topic, key=k, value=v)
	producer.flush()

if __name__ == "__main__":
	conf = SparkConf()
	conf.set("spark.streaming.backpressure.enabled", "true")
	conf.set("spark.streaming.backpressure.initialRate", "10")
	conf.set("spark.streaming.receiver.maxRate", "10")
	conf.set("spark.cores.max", "4")
#	conf.set("spark.executor.cores", "8")
#	conf.set("spark.default.parallelism", "2")

	sc = SparkContext(appName="ECGSparkStreaming3", conf=conf)
	ssc = StreamingContext(sc, 1)

	zkQuorum = 'Niu-Kafka-0:2181,Niu-Kafka-1:2181,Niu-Kafka-2:2181'
	topic = 'stream-input'

#	define 2 receivers
	num_streams = 2 
	kafka_streams = [ KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 4}) for _ in range(num_streams) ]
	union_stream = ssc.union(*kafka_streams)
	lines = union_stream.map(lambda x: x[1])
#	lines.pprint()

	streams = lines.map( mapper )
#	streams = streams.reduceByKey(lambda x, y: "%s %s" % (x, y))
	streams = streams.reduceByKeyAndWindow(lambda x, y: "%s-->%s" % (x, y), None, 10, 2, None)
#	streams.pprint()
	streams.foreachRDD( lambda rdd: rdd.foreach(toKafka) )

	ssc.start()
	ssc.awaitTermination()

