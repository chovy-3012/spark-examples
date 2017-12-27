package spark.jdbc

import org.apache.spark.sql.SparkSession

object MysqlSparkRead {

  def main(args: Array[String]): Unit = {
    //2.0以后新api，可以使用sparksession,跳过sparkconf和sparkcontext
    val sparkSession = SparkSession.builder().appName("MysqlSpark").master("local[2]").getOrCreate()
    //下面一行的read等于new DataFrameReader(sc).read
    val data = sparkSession.read.format("jdbc").options(Map("url" -> "jdbc:mysql://192.168.3.233:3306/test1",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "hotle",
      "user" -> "root", "password" -> "Zw2051300663211138.")).load()
    data.toDF().write.text("hdfs://192.168.2.5:9000/zhaow/hotel.txt")
    val count = data.count()
    println(count)
  }
}
