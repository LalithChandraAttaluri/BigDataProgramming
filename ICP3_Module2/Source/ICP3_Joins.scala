import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
object ICP3_Joins {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.option("header", "true").csv("C:/Users/Lalith Chandra A/Desktop/survey.csv")
    file.printSchema()
    val save1= file
      .write.format("com.databricks.spark.csv")
      .save("C:/Users/Lalith Chandra A/Desktop/saved001")
    file.registerTempTable("survey")
    file.registerTempTable("survey")
    file.registerTempTable("survey1")
    val query3 = sqlContext.sql("select s1.Country,s2.State from survey s1 join survey1 s2 on s1.Country=s2.Country")
    query3.show()
    query3.coalesce(1).write.csv("output_ICP3_Joins")
  }
}
