# BATCH PROCESSING


## Problem

* Automate the ingest tasks and transformation tasks created during yesterday's labs with Ooozie. 
* Use Hive-on-Spark.
* Use SparkSQL.

## Oozie Workflows

In order to import each of four tables from Oracle with sqoop we've created a workflow that calls four subworkflows (one subworkflow for each table):

### Superworkflow configuration

```
<workflow-app name="superworkflow" xmlns="uri:oozie:workflow:0.5">
    <start to="fork-91d0"/>
    <kill name="Kill">
        <message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <action name="subworkflow-e5a5">
        <sub-workflow>
            <app-path>${nameNode}/user/hue/oozie/workspaces/hue-oozie-1498671833.68</app-path>
              <propagate-configuration/>
            <configuration>
                <property>
                    <name>MYTABLE</name>
                    <value>DETECTORS</value>
                </property>
                <property>
                    <name>hue-id-w</name>
                    <value>68</value>
                </property>
            </configuration>
        </sub-workflow>
        <ok to="join-1c39"/>
        <error to="Kill"/>
    </action>
    <action name="subworkflow-8a2c">
        <sub-workflow>
            <app-path>${nameNode}/user/hue/oozie/workspaces/hue-oozie-1498671833.68</app-path>
              <propagate-configuration/>
            <configuration>
                <property>
                    <name>MYTABLE</name>
                    <value>GALAXIES</value>
                </property>
                <property>
                    <name>hue-id-w</name>
                    <value>68</value>
                </property>
            </configuration>
        </sub-workflow>
        <ok to="join-1c39"/>
        <error to="Kill"/>
    </action>
    <fork name="fork-91d0">
        <path start="subworkflow-e5a5" />
        <path start="subworkflow-8a2c" />
        <path start="subworkflow-2af6" />
        <path start="subworkflow-57ca" />
    </fork>
    <join name="join-1c39" to="End"/>
    <action name="subworkflow-2af6">
        <sub-workflow>
            <app-path>${nameNode}/user/hue/oozie/workspaces/hue-oozie-1498671833.68</app-path>
              <propagate-configuration/>
            <configuration>
                <property>
                    <name>MYTABLE</name>
                    <value>ASTROPHYSICISTS</value>
                </property>
                <property>
                    <name>hue-id-w</name>
                    <value>68</value>
                </property>
            </configuration>
        </sub-workflow>
        <ok to="join-1c39"/>
        <error to="Kill"/>
    </action>
    <action name="subworkflow-57ca">
        <sub-workflow>
            <app-path>${nameNode}/user/hue/oozie/workspaces/hue-oozie-1498671833.68</app-path>
              <propagate-configuration/>
            <configuration>
                <property>
                    <name>MYTABLE</name>
                    <value>MEASUREMENTS</value>
                </property>
                <property>
                    <name>hue-id-w</name>
                    <value>68</value>
                </property>
            </configuration>
        </sub-workflow>
        <ok to="join-1c39"/>
        <error to="Kill"/>
    </action>
    <end name="End"/>
</workflow-app>
```

### Subworkflow configuration

```
<workflow-app name="dima" xmlns="uri:oozie:workflow:0.5">
    <start to="sqoop-b4a9"/>
    <kill name="Kill">
        <message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <action name="sqoop-b4a9">
        <sqoop xmlns="uri:oozie:sqoop-action:0.2">
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
            <command>import --connect jdbc:oracle:thin:@gravity.cghfmcr8k3ia.us-west-2.rds.amazonaws.com:15210:gravity --username gravity --password bootcamp  --hive-import --table ${MYTABLE} --create-hive-table --hive-database  dfadeyev --hive-table ${MYTABLE} --direct --target-dir=/tmp/${MYTABLE}</command>
            <file>/tmp/hive-site.xml#hive-site.xml</file>
        </sqoop>
        <ok to="End"/>
        <error to="Kill"/>
    </action>
    <end name="End"/>
</workflow-app>
```

### Hive transformation workflow configuration 

```
<workflow-app name="Denormalize" xmlns="uri:oozie:workflow:0.5">
    <start to="hive2-cdd0"/>
    <kill name="Kill">
        <message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <action name="hive2-cdd0" cred="hive2">
        <hive2 xmlns="uri:oozie:hive2-action:0.1">
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
            <jdbc-url>jdbc:hive2://ip-172-31-43-82.us-west-2.compute.internal:10000/default</jdbc-url>
            <script>/tmp/hive3.sql</script>
        </hive2>
        <ok to="End"/>
        <error to="Kill"/>
    </action>
    <end name="End"/>
</workflow-app>

```
###Hive queries used to change datatypes:

Fact table:
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
INSERT OVERWRITE TABLE measurements_partitioned PARTITION(galaxy_id) select measurement_id, cast(detector_id as int), cast(astrophysicist_id as int), cast(measurement_time as int), cast(amplitude_1 as decimal(24,20)), cast(amplitude_2 as decimal(24,20)), cast(amplitude_3 as decimal(24,20)), cast(galaxy_id as int) FROM measurements;
```

Dimensional tables:
```
create table astrophysicists_prod stored as parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY') as select cast(astrophysicist_id as int), astrophysicist_name, cast(year_of_birth as int), nationality from astrophysicists;
create table detectors_prod stored as parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY') as select cast(detector_id as int), detector_name, country, cast(latitude as double), cast(longitude as double) from detectors;
create table galaxies_prod stored as parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY') as select cast(galaxy_id as int), galaxy_name, galaxy_type, cast(distance_ly as double), cast(absolute_magnitude as double), cast(apparent_magnitude as double), galaxy_group from galaxies;

```
Hive query used to apply all transformations:

```
select count(*) from (select 
measurement_id, 
measurement_time, 
amplitude_1, 
amplitude_2, 
amplitude_3,
detector_name, 
country, 
latitude, 
longitude,
galaxy_name, 
galaxy_type, 
distance_ly, 
absolute_magnitude, 
apparent_magnitude, 
galaxy_group,
astrophysicist_name, 
year_of_birth, 
nationality,
case when measurements_partitioned.amplitude_1 > 0.995 and
          measurements_partitioned.amplitude_3 > 0.995 and 
          measurements_partitioned.amplitude_2 < 0.005 then 'Y' else 'N' end gwave_flag
from measurements_partitioned
inner join detectors_prod
on detectors_prod.detector_id = measurements_partitioned.detector_id
inner join astrophysicists_prod
on astrophysicists_prod.astrophysicist_id = measurements_partitioned.astrophysicist_id
inner join galaxies_prod
on galaxies_prod.galaxy_id = measurements_partitioned.galaxy_id) aaa
```

# Hive-on-Spark

Parameters (hive parameters, not spark ones) that have been tuned before activating Hive-on-Spark:

```
Spark Driver Memory Overhead: 26MB -> 400MB
Spark Executor Maximum Java Heap Size: 256MB -> 4GB
Spark Executor Memory Overhead: 26MB -> 400MB
Spark Driver Maximum Java Heap Size: 256MB -> 4GB
```

The engine change itself has been done on command line (in beeline):
```
set hive.execution.engine=spark;
```

It took almos the same time for the 'transformations' query to complete on each engine, spark being slightly faster then MR:
Spark    131 seconds
MR       145 seconds

# Data type transformation using Spqrk SQL

Python:

```
from pyspark import SparkContext
from pyspark.sql import HiveContext
sc = SparkContext()
sqlCtx = HiveContext(sc)
sqlCtx.setConf("spark.sql.parquet.compression.codec.", "snappy")
sqlCtx.sql("create table mohit.galaxies_prod stored as parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY') as select cast(galaxy_id as int), galaxy_name, galaxy_type, cast(distance_ly as double), cast(absolute_magnitude as double), cast(apparent_magnitude as double), galaxy_group from dfadeyev.galaxies")

sudo -u hive spark-submit /tmp/hivesql.py
```

Scala spark-shell script:
```
sqlContext.sql("""
select count(*) from (select 
measurement_id, 
measurement_time, 
amplitude_1, 
amplitude_2, 
amplitude_3,
detector_name, 
country, 
latitude, 
longitude,
galaxy_name, 
galaxy_type, 
distance_ly, 
absolute_magnitude, 
apparent_magnitude, 
galaxy_group,
astrophysicist_name, 
year_of_birth, 
nationality,
case when measurements_partitioned.amplitude_1 > 0.995 and
          measurements_partitioned.amplitude_3 > 0.995 and 
          measurements_partitioned.amplitude_2 < 0.005 then 'Y' else 'N' end gwave_flag
from measurements_partitioned
inner join detectors_prod
on detectors_prod.detector_id = measurements_partitioned.detector_id
inner join astrophysicists_prod
on astrophysicists_prod.astrophysicist_id = measurements_partitioned.astrophysicist_id
inner join galaxies_prod
on galaxies_prod.galaxy_id = measurements_partitioned.galaxy_id) aaa
""").show()
```
