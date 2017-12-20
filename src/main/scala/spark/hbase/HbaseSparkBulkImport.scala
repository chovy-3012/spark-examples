package spark.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._

object HbaseSparkBulkImport {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "ht05")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val tableName = "spark_hbase"
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val table = new HTable(conf, tableName)

    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, table)

    //从txt写入到hbase
    val dataRdd = sc.textFile("hdfs://ht05:9000//zhaowei/pass.txt")
    val rdd = dataRdd.sortBy(x=>x).map(line => {
      var rowkey: String = ""
      if (line.length == 0) {
        rowkey = "1"
      } else {
        rowkey = line
      }
      val kv: KeyValue = new KeyValue(Bytes.toBytes(rowkey), "cf".getBytes(), "name".getBytes(), rowkey.getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), kv)
    })
    rdd.saveAsNewAPIHadoopFile("hdfs://ht05:9000/tmp/iteblog", classOf[ImmutableBytesWritable], classOf[KeyValue],
      classOf[HFileOutputFormat2], conf)

    //Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("hdfs://ht05:9000/tmp/iteblog"), table)
  }
}
