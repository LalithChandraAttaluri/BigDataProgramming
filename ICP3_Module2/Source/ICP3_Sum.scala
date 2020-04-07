import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
object ICP3_Sum {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.option("header", "true").csv("C:/Users/Lalith Chandra A/Desktop/survey.csv")
    val save1 = file
      .write.format("com.databricks.spark.csv")
      .save("C:/Users/Lalith Chandra A/Desktop/saved0004")
    file.registerTempTable("survey")
    val query3 = sqlContext.sql("select SUM(Age) from survey")
    query3.show()
  }
}
