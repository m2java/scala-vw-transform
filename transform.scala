val path = sys.env("FILE_PATH")
val table = sys.env("TABLE_NAME")
val df = spark.read.parquet(path)
import org.apache.spark.sql.types._
val df1 = df.withColumn("tsTmp", df("ts").cast(StringType)).drop("ts").withColumnRenamed("tsTmp", "timestamp")
val df2 = df1.withColumn("timezoneTmp", df("timezone").cast(StringType)).drop("timezone").withColumnRenamed("timezoneTmp", "timezone")
import org.apache.spark.sql.functions.{concat, lit}
val df3 = df2.withColumn("col0", concat(lit("def-"), $"userId", lit("--"), $"timestamp", lit("-"), $"type", lit("-")))
val df4 = df3.drop("user_partition","device_partition","date_partition")

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("departure", StringType, true), StructField("destination", StringType, true), StructField("start_tz", StringType, true), StructField("alt", StringType, true), StructField("lon", StringType, true), StructField("lat", StringType, true)))
val df5 = df4.withColumn("json", from_json($"attributes", schema))
val df6 = df5.withColumn("departure", $"json.departure").withColumn("destination", $"json.destination").withColumn("start_tz", $"json.start_tz").withColumn("alt", $"json.alt").withColumn("lon", $"json.lon").withColumn("lat", $"json.lat").drop("json").drop("attributes")

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
df6.rdd.foreach(record => {
    val i = scala.util.Random.nextInt(10000)+1
    val hConf = new HBaseConfiguration() 
    val hTable = new HTable(hConf, table) 
    val put = new Put(Bytes.toBytes(record.getString(5)+i)) 
    put.add(Bytes.toBytes("a"), Bytes.toBytes("user_id"), Bytes.toBytes(record.getString(0))) 
    put.add(Bytes.toBytes("a"), Bytes.toBytes("device_id"), Bytes.toBytes(record.getString(1))) 
    put.add(Bytes.toBytes("a"), Bytes.toBytes("type"), Bytes.toBytes(record.getString(2))) 
    put.add(Bytes.toBytes("a"), Bytes.toBytes("timestamp"), Bytes.toBytes(record.getString(3))) 
    put.add(Bytes.toBytes("a"), Bytes.toBytes("timezone"), Bytes.toBytes(record.getString(4)))
    
    if (record.getString(6) != null)
    put.add(Bytes.toBytes("a"), Bytes.toBytes("departure"), Bytes.toBytes(record.getString(6)))
    
    if (record.getString(7) != null)
    put.add(Bytes.toBytes("a"), Bytes.toBytes("destination"), Bytes.toBytes(record.getString(7)))
    
    put.add(Bytes.toBytes("a"), Bytes.toBytes("start_tz"), Bytes.toBytes(record.getString(8))) 
    put.add(Bytes.toBytes("a"), Bytes.toBytes("alt"), Bytes.toBytes(record.getString(9))) 
    put.add(Bytes.toBytes("a"), Bytes.toBytes("lon"), Bytes.toBytes(record.getString(10))) 
    put.add(Bytes.toBytes("a"), Bytes.toBytes("lat"), Bytes.toBytes(record.getString(11)))
    hTable.put(put)
  })
sys.exit
