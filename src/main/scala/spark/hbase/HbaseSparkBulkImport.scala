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
    val tableName = "spark_test"
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val table = new HTable(conf, tableName)

    lazy val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, table)

    //从txt写入到hbase
    val dataRdd = sc.textFile("hdfs://ht05:9000//zhaow/hotels/*")
    // val dataRdd = sc.makeRDD(1 to 100).map(x => x.toString)
    val rdd = dataRdd.filter(_.length() > 0).sortBy(x => x, true).map(line => {
      val rowkey = line
      val kv: KeyValue = new KeyValue(Bytes.toBytes(rowkey), "cf".getBytes(), "name".getBytes(), rowkey.getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), kv)
    })
    rdd.saveAsNewAPIHadoopFile("hdfs://ht05:9000/tmp/test3", classOf[ImmutableBytesWritable], classOf[KeyValue],
      classOf[HFileOutputFormat2], job.getConfiguration)

    //Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("hdfs://ht05:9000/tmp/test3"), table)
  }
}
