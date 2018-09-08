import org.apache.spark.{SparkConf, SparkContext}

object SavedNeo4j {

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("OneId Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

  }


}