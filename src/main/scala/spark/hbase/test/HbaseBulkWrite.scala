package spark.hbase.test

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

object HbaseBulkWrite {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("hbase_regions").getOrCreate()
    val sc = spark.sparkContext

    //hbase信息
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "ht05")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "hbase_regions")

    //lazy，延迟加载，如果程序在spark-shell里面运行必须使用延迟加载，因为spark-shell里面每一部都会打印结果
    lazy val job = new Job(sc.hadoopConfiguration)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])

    val dataRdd = sc.makeRDD(1 to (2000000000))
    val rdd = dataRdd.map { line => {
      val rowkey = java.util.UUID.randomUUID().toString
      val put = new Put(Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name1"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name2"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name3"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name4"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name5"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name6"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name7"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name8"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name9"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name10"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name11"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name12"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name13"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name14"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name15"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name16"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name17"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name18"), Bytes.toBytes(rowkey + rowkey))
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("name19"), Bytes.toBytes(rowkey + rowkey))
      (new ImmutableBytesWritable, put)
    }
    }
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
