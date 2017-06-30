# Problem

* Run measurements stream using the provided data generator
* Build a “generator -> Flume -> HBase” pipeline (switch out HBase for HDFS)
* Build a “generator -> Flume -> Kafka -> Spark Streaming -> HBase” pipeline
* Switch out HBase for Kudu

# Run measurements stream using the provided data generator

Build the generator to create package jar
```
mvn clean package -DskipTests
```

Run the generator that writes on port 9999. Flume agent(s) are listening on this host:port
```
java -cp /tmp/bootcamp-0.0.1-SNAPSHOT.jar com.cloudera.fce.bootcamp.MeasurementGenerator localhost 9999
```

# Build a “generator -> Flume -> HBase” pipeline

Configure Flume agent in CM to listen to generator and sink into HBase. Then restart Flume.

```
tier1.sources  = source1
tier1.channels = channel1
tier1.sinks    = sink1

# For each source, channel, and sink, set
# standard properties.
tier1.sources.source1.type     = netcat
tier1.sources.source1.bind     = 127.0.0.1
tier1.sources.source1.port     = 9999
tier1.sources.source1.channels = channel1

tier1.channels.channel1.type   = memory
tier1.channels.channel1.capacity = 1000000

tier1.sinks.sink1.type         = hbase
tier1.sinks.sink1.table        = t1
tier1.sinks.sink1.columnFamily =f1
tier1.sinks.sink1.channel      = channel1
```

# Build a “generator -> Flume -> HDFS” pipeline

Configure Flume agent in CM to listen to generator and sink into HDFS. Then restart Flume.
```
tier1.sources  = source1
tier1.channels = channel1
tier1.sinks    = sink1

# For each source, channel, and sink, set
# standard properties.
tier1.sources.source1.type     = netcat
tier1.sources.source1.bind     = 127.0.0.1
tier1.sources.source1.port     = 9999
tier1.sources.source1.channels = channel1

tier1.channels.channel1.type   = memory
tier1.channels.channel1.capacity = 1000000

tier1.sinks.sink1.type         = hdfs
tier1.sinks.sink1.hdfs.path = /tmp
tier1.sinks.sink1.channel      = channel1
```

# Build a “generator -> Flume -> Kafka -> Spark Streaming -> HBase” pipeline 

Configure Flume agent in CM to listen to generator and sink into Kafka. Then restart Flume.

```
tier1.sources  = source1
tier1.channels = channel1
tier1.sinks    = sink1

# For each source, channel, and sink, set
# standard properties.
tier1.sources.source1.type     = netcat
tier1.sources.source1.bind     = 127.0.0.1
tier1.sources.source1.port     = 9999
tier1.sources.source1.channels = channel1

tier1.channels.channel1.type   = memory
tier1.channels.channel1.capacity = 1000000

tier1.sinks.sink1.type         = org.apache.flume.sink.kafka.KafkaSink
tier1.sinks.sink1.kafka.bootstrap.servers    = ip-172-31-32-132.us-west-2.compute.internal:9092
tier1.sinks.sink1.channel      = channel1
```

Spark Streaming app was written in Python. In order to write into HBase we used the Hadoop OutputFormat API. This entailed using Python libraries to types that are required by Hadoop input and output format.

Provide Spark with typer convertor and HBase libraries. In CM Spark Configuration Safety Valve for spark-default properties (client level configuration), add the following
```
spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/spark/lib/spark-examples-1.6.0-cdh5.11.1-hadoop2.6.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-common-1.2.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-client-1.2.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-protocol-1.2.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-server-1.2.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/guava-12.0.1.jar

spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/spark/lib/spark-examples-1.6.0-cdh5.11.1-hadoop2.6.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-common-1.2.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-client-1.2.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-protocol-1.2.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-server-1.2.0-cdh5.11.1.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/guava-12.0.1.jar
```

We wrote the following Python spark streaming script to stream records from Kafka to HBase

```
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext

def toHBase(time, rdd):
   hbaseZkQuorum = 'ip-172-31-43-82.us-west-2.compute.internal'
   table = 't2'
   keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
   valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
   conf = {"hbase.zookeeper.quorum": hbaseZkQuorum,
           "hbase.mapred.outputtable": table,
           "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
           "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
           "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
   rdd.map(lambda x : x[1].split(',')).map(lambda x : (str(x[0]), [str(x[0]), "f2", "myrecord", ','.join(x)])).saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)

if __name__ == "__main__":
   sc = SparkContext()
   ssc = StreamingContext(sc, 10)
   mystream = KafkaUtils.createStream(ssc, 'ip-172-31-43-82.us-west-2.compute.internal:2181', "spark-streaming-consumer", {'default-flume-topic': 1})
   mystream.count().map(lambda x :'Records in this batch: %s' % x).pprint()
   mystream.foreachRDD(toHBase)

   ssc.start()
   ssc.awaitTermination()
   ssc.stop()
```

Note that in HBase, row key is the measurementID and value is comma separated data received from Kafka.


# Debugging

To confirm data is getting into Kafka, we used command line tools like:

```
kafka-topics --zookeeper ip-172-31-43-82.us-west-2.compute.internal:2181 --list

kafka-console-consumer --zookeeper  ip-172-31-43-82.us-west-2.compute.internal:2181  --topic default-flume-topic  --from-beginning
```
