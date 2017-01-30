# master_thesis

In the master thesis, three main stream Real-Time Data Analytic Technologies are compared. They are Spark Streaming, Kafka Stream, Storm. The source codes of each technology in this repository are all programed based on a same data model(data flow) to have a common programming logic for them to compare within.

# data model
1.	Read data from Kafka
2.	Transform data into other format
3.	Group by key (Patient ID)
4.	Set time-slide-window
5.	Apply algorithm (RPeak Detection)
6.	Aggregation (Reduce, Join)
7.	Output (Kafka, Database, HDFS, REST API)
