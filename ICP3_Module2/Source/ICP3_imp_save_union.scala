import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
object ICP3_imp_save_union {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.option("header", "true").csv("C:/Users/Lalith Chandra A/Desktop/survey.csv")
    file.printSchema()

    val save1 = file
      .write.format("com.databricks.spark.csv")
      .save("C:/Users/Lalith Chandra A/Desktop/saved000")
    file.registerTempTable("survey")
    file.registerTempTable("survey")

    val query1 = sqlContext.sql("select * from survey where care_options like '%Yes' union select * from survey where care_options like '%No' order by Country")
    println("Union Query Executed!")
    val query2 = sqlContext.sql("select treatment,count(*) from survey group by treatment")
    println("GROUPBY Query Executed!")

    query1.coalesce(1).write.csv("output_ICP3_Union")
    query1.coalesce(1).write.csv("output_ICP3_Groupby")
    query1.show()
    query2.show()

  }
}
