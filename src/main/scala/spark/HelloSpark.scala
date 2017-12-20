package spark

import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark {
  def main(args: Array[String]): Unit = {
    //初始化sparkconf,设置spark appname，以及master，这两个是必须的
    val conf=new SparkConf().setAppName("HelloSpark").setMaster("local[4]");
    conf.set("spark.hadoop.yarn.resourcemanager.hostname","ht05")
    //由conf初始化sparkcontext
    val sc=new SparkContext(conf);
    //从hdfs地址读取一个文件
    val textFile1=sc.textFile("hdfs://192.168.2.5:9000/zhaowei/pass.txt",4)
    val textFile=textFile1.cache()
    //1.count行数
    val count=textFile.count()
    println("count:"+count)
    //2.计算单词数
    val flatMapRdd=textFile.flatMap(line=>line.split(" "))
    println("words count:"+flatMapRdd.count())
  }
}
