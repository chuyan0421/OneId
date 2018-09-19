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
      .executeQuery("select user_id,email,phone from dtplatform.dim_us_user") ///limit 20000

    val fetchRes = mutable.MutableList[StatsRec]()
    while (res.next()){
      var rec = StatsRec(res.getString("user_id"),
        res.getString("email"),
        res.getString("phone"))
      fetchRes += rec

    }
    conn.close()

    import sqlContext.implicits._
    val fetchResDF:DataFrame = fetchRes.toDF()
    //    println(fetchResDF.getClass())
    //    fetchResDF.show()
    return fetchResDF

  }

  case class StatsRec2(
                       user_id:String,
                       device_id:String
                     )

  def getHiveDataframe2(sqlContext:SQLContext):DataFrame = {
    val conn = DriverManager.getConnection("jdbc:hive2://192.168.33.25:10000","","")
    val fetchRes = mutable.MutableList[StatsRec2]()

    val res2 = conn.createStatement()
      .executeQuery("select user_id,device_id from dtplatform.dwd_log_view_page_di")

    while (res2.next()){
      var rec2 = StatsRec2(res2.getString("user_id"),
        res2.getString("device_id"))
      fetchRes += rec2
    }

    val res3 = conn.createStatement()
      .executeQuery("select user_id,device_id from dtplatform.dwd_log_view_item_di")

    while (res3.next()){
      var rec3 = StatsRec2(res3.getString("user_id"),
        res3.getString("device_id"))
      fetchRes += rec3
    }

    val res4 = conn.createStatement()
      .executeQuery("select user_id,device_id from dtplatform.dwd_log_usr_login_di")

    while (res4.next()){
      var rec4 = StatsRec2(res4.getString("user_id"),
        res3.getString("device_id"))
      fetchRes += rec4
    }

    //dwd_log_search_keyword_di

    val res5 = conn.createStatement()
      .executeQuery("select user_id,device_id from dtplatform.dwd_log_search_keyword_di")

    while (res5.next()){
      var rec5 = StatsRec2(res5.getString("user_id"),
        res3.getString("device_id"))
      fetchRes += rec5
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