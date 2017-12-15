package spark.jdbc

import org.apache.spark.sql.SparkSession

object MysqlSparkRead {

  def main(args: Array[String]): Unit = {
    //2.0以后新api，可以使用sparksession,跳过sparkconf和sparkcontext
    val sparkSession = SparkSession.builder().appName("MysqlSpark").master("local[2]").getOrCreate()
    //下面一行的read等于new DataFrameReader(sc).read
    val data = sparkSession.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3306/report",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "(select * from device_stat order by createtime limit 100) as aaa",
      "user" -> "root", "password" -> "Zw2051300663211138.")).load()
    val count = data.count()
    println(count)
  }
}
