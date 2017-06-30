```
import --connect "jdbc:oracle:thin:@gravity.cghfmcr8k3ia.us-west-2.rds.amazonaws.com:15210:gravity"   --username gravity --password bootcamp  --compression-codec org.apache.hadoop.io.compress.SnappyCodec     --as-parquetfile  --table ${TABLE}  --hive-database mohit --hive-table ${TABLE} --hive-import  --create-hive-table --direct
```

# Data type transformation using Spqrk SQL

```
from pyspark import SparkContext
from pyspark.sql import HiveContext
sc = SparkContext()
sqlCtx = HiveContext(sc)
sqlCtx.setConf("spark.sql.parquet.compression.codec.", "snappy")
sqlCtx.sql("create table mohit.galaxies_prod stored as parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY') as select cast(galaxy_id as int), galaxy_name, galaxy_type, cast(distance_ly as double), cast(absolute_magnitude as double), cast(apparent_magnitude as double), galaxy_group from dfadeyev.galaxies")

sudo -u hive spark-submit /tmp/hivesql.py
```
