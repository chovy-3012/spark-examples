package spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._


object HbaseSparkRead {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    //hbase信息
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "ht05")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, "spark_hbase")


    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //val count = hBaseRDD.count()
    //println(count)
    hBaseRDD.foreach { case (_, result) => {
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf".getBytes, "name".getBytes))
      println("Row key:" + key + " Name:" + name)
    }
    }

    //将hbase数据保存到txt
    hBaseRDD.map(x => Bytes.toString(x._2.getRow)).saveAsTextFile("hdfs://ht05:9000/test1")
  }
}
