# Problem

Ingest data from Oracle and make the tables available to Impala for querying in Hue.

Fact table called 'measurements' has 500 million rows. Reference tables are: detectors (8 rows), galaxies (128 rows), astrophysicists (106 rows)

# Important Considerations

* Import parallelism
* Write directly to Parquet
* Compression
* Ingest straight into a partitioned table

# Ingest using Sqoop

Ingesting from Sqoop directly into Hive using --split-by was too slow because Sqoop issues a MIN/MAX call to Oracle on a non-indexed column was very slow. So we chose to ingest into HDFS un-partitioned.  

Additionally, to make import faster:
* added Oracle specific --direct flag to Sqoop import
* specified number of mappers as 20 to Sqoop import

```
sudo -u hive sqoop import --connect "jdbc:oracle:thin:@gravity.cghfmcr8k3ia.us-west-2.rds.amazonaws.com:15210:gravity"     --username gravity --password bootcamp     compress --compression-codec snappy     --as-parquetfile     --table MEASUREMENTS    --num-mappers 20  --hive-import --hive-partition-key galaxy_id --create-hive-table --direct
```

Next, we ran a Hive query to do dynamic partitioning.

Since each mapper trigged by Hive writes to multiple partition files, each in Parquet format, there is large memory pressure on the mappers to accomodate multiple ParquetWriter buffers. We had to increase mapper memory to accomodate increased memory usage.

```
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=200;
set hive.exec.max.dynamic.partitions=200;
set mapreduce.map.java.opts=-Xmx1500m;
set mapreduce.map.memory.mb=2048;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
use dfadeyev;

create table measurements_partitioned(measurement_id string, detector_id int, astrophysicist_id int, measurement_time int, amplitude_1 decimal(24,20), amplitude_2 decimal(24,20), amplitude_3 decimal(24,20)) PARTITIONED BY (galaxy_id int) STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

INSERT OVERWRITE TABLE measurements_partitioned PARTITION(galaxy_id) select measurement_id, cast(detector_id as int), cast(astrophysicist_id as int), cast(measurement_time as int), cast(amplitude_1 as decimal(24,20)), cast(amplitude_2 as decimal(24,20)), cast(amplitude_3 as decimal(24,20)), cast(galaxy_id as int) FROM measurements
```
 
# Debugging 

To debug, we looked at aggregated logs using YARN UIs and on the command line tools like :

```
yarn application --list -appStates ALL | grep -i running

sudo -u hive yarn logs -applicationId application_1498550066225_0023 | grep -i xmx

sudo -u hdfs hdfs dfs -du -h /user/hive/warehouse/measurements_partitioned/*
```
