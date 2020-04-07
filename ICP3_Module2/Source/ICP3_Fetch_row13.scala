import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
object ICP3_Fetch_row13 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.format("com.databricks.spark.csv").load("C:/Users/Lalith Chandra A/Desktop/survey.csv")
    val save1 = file
      .write.format("com.databricks.spark.csv")
      .save("C:/Users/Lalith Chandra A/Desktop/saved0002")
    file.registerTempTable("survey")
    file.registerTempTable("survey")
    val row = file.rdd.take(13).last
    print(row)
  }
}
