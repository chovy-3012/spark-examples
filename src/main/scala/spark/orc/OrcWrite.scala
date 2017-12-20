package spark.orc

import org.apache.spark.{SparkConf, SparkContext}

object OrcWrite {
  def main(args: Array[String]): Unit = {
    //init spark
    val sparkConf = new SparkConf().setAppName("sparkApp").setMaster("local")
    val sc = new SparkContext(sparkConf)

    //read data from txt
    val textFile = sc.textFile("hdfs://ht05:9000/zhaow/pass.txt")
  }
}
