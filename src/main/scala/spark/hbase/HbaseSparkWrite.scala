package spark.hbase

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._

object HbaseSparkWrite {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    //从txt写入到hbase
    val dataRdd = sc.textFile("hdfs://ht05:9000//zhaowei/pass.txt")

    //hbase信息
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "ht05")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "spark_hbase")

    //lazy，延迟加载，如果程序在spark-shell里面运行必须使用延迟加载，因为spark-shell里面每一部都会打印结果
    lazy val job = new Job(sc.hadoopConfiguration)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])

    val rdd = dataRdd.map { line => {
      var rowkey: String = ""
      if (line.length == 0) {
        rowkey = "1"
      } else {
        rowkey = line
      }
      val put = new Put(Bytes.toBytes(rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name1"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name2"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name3"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name4"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name5"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name6"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name7"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name8"), Bytes.toBytes(rowkey))
      //      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name9"), Bytes.toBytes(rowkey))
      (new ImmutableBytesWritable, put)
    }
    }
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
