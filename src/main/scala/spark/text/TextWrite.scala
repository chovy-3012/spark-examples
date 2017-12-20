package spark.text

import org.apache.spark.{SparkConf, SparkContext}

object TextWrite {
  def main(args: Array[String]): Unit = {
    //init spark
    val sparkConf = new SparkConf().setAppName("sparkApp").setMaster("local")
    val sc = new SparkContext(sparkConf)

    //加载txt文件
    val textFile = sc.textFile("hdfs://ht05:9000/zhaow/pass.txt")

    //默认写入文件的数量等于加载上面文件的任务数量
    textFile.saveAsTextFile("hdfs://ht05:9000/zhaow/test")

    //这里可以选择reduce的数量，来控制生成txt文件的数量
    textFile.repartition(2).saveAsTextFile("hdfs://ht05:9000/zhaow/test")
  }
}
