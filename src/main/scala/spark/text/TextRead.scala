package spark.text

import org.apache.spark.{SparkConf, SparkContext}

object TextRead {
  def main(args: Array[String]): Unit = {
    //init spark
    val sparkConf = new SparkConf().setAppName("sparkApp").setMaster("local")
    val sc = new SparkContext(sparkConf)

    //加载txt文件
    val textFile = sc.textFile("hdfs://ht05:9000/zhaow/pass.txt")

    val count=textFile.count()
    print(count)
  }
}
