package spark.jdbc

import org.apache.spark.{SparkConf, SparkContext}

object Mysql {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("HelloSpark").setMaster("local[4]");
    val sc=new SparkContext(conf)
  }
}
