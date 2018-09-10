import java.sql.DriverManager

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

object HiveOperation{

  case class StatsRec(
                     user_id:String,
                     email:String,
                     phone:String
                     )

  def getHiveDataframe(sqlContext:SQLContext):DataFrame = {
    val conn = DriverManager.getConnection("jdbc:hive2://192.168.33.25:10000","","")
    val res = conn.createStatement()
      .executeQuery("select USER_ID,EMAIL,PHONE from dtplatform.DIM_US_USER limit 2000")

    val fetchRes = mutable.MutableList[StatsRec]()
    while (res.next()){
      var rec = StatsRec(res.getString("USER_ID"),
        res.getString("EMAIL"),
        res.getString("PHONE"))
      fetchRes += rec

    }
    conn.close()

    import sqlContext.implicits._
    val fetchResDF:DataFrame = fetchRes.toDF()
    //    println(fetchResDF.getClass())
    //    fetchResDF.show()
    return fetchResDF

  }


//  def main(args:Array[String])={
//
//    val conf = new SparkConf().setMaster("local[*]").setAppName("HiveTest")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
////
//
//    val conn = DriverManager.getConnection("jdbc:hive2://192.168.33.25:10000","","")
//    val res = conn.createStatement()
//      .executeQuery("select USER_ID,EMAIL,PHONE from dtplatform.DIM_US_USER limit 30")
//
//    val fetchRes = mutable.MutableList[StatsRec]()
//    while (res.next()){
//      var rec = StatsRec(res.getString("USER_ID"),
//        res.getString("EMAIL"),
//        res.getString("PHONE"))
//      fetchRes += rec
//
//    }
//    conn.close()
////    val rddStatsDelta = sc.parallelize(fetchRes)
////    rddStatsDelta.foreach(println(_))
//    import sqlContext.implicits._
//    val fetchResDF = fetchRes.toDF()
//    println(fetchResDF.getClass())
//    fetchResDF.show()
//
//  }


}