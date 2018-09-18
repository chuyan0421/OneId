/* SimpleApp.scala */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.sql.{DataFrame, SQLContext}
import util._
import HiveOperation._
import HbaseOperation._
import org.apache.spark.sql.hive.HiveContext

object OneId {

    def main(args: Array[String]) {

        val conf = new SparkConf().setMaster("local[*]").setAppName("OneId Application")
        val sc = new SparkContext(conf)

        //    val path = "/E:/oneID/oneId_test.txt" // test file path
        //    val path = "/E:/oneID/data.log.2018-08-13.002" //
        val pathApp = "/yanzi/oneId_test.txt"
        val pathImarking = "hdfs://192.168.33.25:8020/data/dtplatform/sass/ods/ods_act_click_log.parquet/"


        val sqlContext = new SQLContext(sc)

        ///数据预处理，删除空值,$开头的节点在sql会报错
        val preData = sc.textFile(pathApp)
          .filter(line => line.contains("未取到值") == false)
          .map(line => line.replaceAll("\\$",""))
          .map(line => line.replaceAll("00000000-0000-0000-0000-000000000000"," "))

        val appOfData = preData
          .filter(line => line.contains("browser") == false) /// 与pc埋点数据分开处理
          .filter(line => line.contains("PC端商城") == false) /// 非PC端商城
        println("number of valid APP data is: " + appOfData.count())

        val appSql = sqlContext.read.json(appOfData)
        appSql.registerTempTable("appSql")
        val appData = sqlContext
          .sql("SELECT properties.login_id, properties.device_id, " +
            "properties.mac, properties.idfa, distinct_id FROM appSql")

//    val pcOfData = preData
//      .filter(line => line.contains("browser")) /// pc埋点数据
//      .filter(line => line.contains("PC端商城")) /// PC端商城
//    println("number of valid PC data is: " + pcOfData.count())
//
//    val pcSql = sqlContext.read.json(pcOfData)
//    pcSql.registerTempTable("pcSql")
//    val pcData = sqlContext
//      .sql("SELECT properties.user_id, properties.device_id, " +
//        "properties.cookie_id, distinct_id FROM pcSql")

        //// connect remote hdfs
        val imarkingSql = sqlContext.read.parquet(pathImarking)
        imarkingSql.registerTempTable("imarkingSql")
        val imarkingData = sqlContext
          .sql("SELECT REAL_USER_ID,USER_PHONE,CURRENT_ID FROM imarkingSql")

        val isalesData = getHiveDataframe(sqlContext) ////jdbc connect remote

        ///connect remote hive
//        val hiveContext = new HiveContext(sc)
//        hiveContext.setConf("hive.metastore.uris", "thrift://192.168.33.25:9083")
//        hiveContext.sql("use dtplatform")
//        val isalesData=hiveContext.sql("select user_id,email,phone from dtplatform.dim_us_user limit 2000")


        /* vertices initialization  */
        /// android can get mac, iphone can get idfa, distinct_id may be telephone number
        val userId = createVertexFromDataframe(appData,"login_id","appUserId")
        val deviceId = createVertexFromDataframe(appData,"device_id","session_id")
        val mac = createVertexFromDataframe(appData,"mac","mac")
        val idfa = createVertexFromDataframe(appData,"idfa","idfa")
        val phone = createVertexFromDataframe(appData,"distinct_id","phone")

        //    val pcUserId = createVertexFromDataframe(pcData,"user_id","user_id")
        //    val pcDeviceId = createVertexFromDataframe(pcData,"device_id","session_id")
        //    val pcCookieId = createVertexFromDataframe(pcData,"cookie_id","cookie_id")
        //    val pcDistinctId = createVertexFromDataframe(pcData,"distinct_id","distinct_id")

        val imUserId = createVertexFromDataframe(imarkingData,"REAL_USER_ID","imUserId")
        val imPhone = createVertexFromDataframe(imarkingData,"USER_PHONE","phone")
        val imOpenId = createVertexFromDataframe(imarkingData,"CURRENT_ID","openId")

        val isUserId = createVertexFromDataframe(isalesData,"user_id","isUserId")
        //    val isEmail = createVertexFromDataframe(isalesData,"email","isEmail")
        val isPhone = createVertexFromDataframe(isalesData,"phone","phone")


        /* edge initialization  */
        val  edgeRDD1 = createEdgeFromDataframe(appData,"device_id","login_id","maotai")
        val  edgeRDD2 = createEdgeFromDataframe(appData,"device_id","mac","maotai")
        val  edgeRDD3 = createEdgeFromDataframe(appData,"device_id","idfa","maotai")
        val  edgeRDD4 = createEdgeFromDataframe(appData,"device_id","distinct_id","maotai")

        //    val  edgeRDD5 = createEdgeFromDataframe(pcData,"device_id","user_id","device_login")
        //    val  edgeRDD6 = createEdgeFromDataframe(pcData,"device_id","cookie_id","device_cookie")
        //    val  edgeRDD7 = createEdgeFromDataframe(pcData,"device_id","distinct_id","device_distinct")

        val  edgeRDD8 = createEdgeFromDataframe(imarkingData,"REAL_USER_ID","USER_PHONE","imarking")
        val  edgeRDD9 = createEdgeFromDataframe(imarkingData,"CURRENT_ID","USER_PHONE","imarking")

        val edgeRDD10 = createEdgeFromDataframe(isalesData,"user_id","phone","isales")
        //    val edgeRDD11 = createEdgeFromDataframe(isalesData,"email","phone","email_phone")

        ///construct  graph
        val vertexRDD = userId.union(deviceId).union(mac).union(idfa).union(phone)
          .union(imUserId).union(imPhone).union(imOpenId)
          .union(isUserId).union(isPhone)
        //      .union(pcDeviceId)
        //    verticeRDD.collect().foreach(println(_))
        val relationRDD = edgeRDD1.union(edgeRDD2).union(edgeRDD3).union(edgeRDD4)
          .union(edgeRDD8).union(edgeRDD9)
          .union(edgeRDD10)

        //    println(relationRDD.first())
        val defaultVertex = ("Missing","Missing")
        val graph = Graph(vertexRDD,relationRDD,defaultVertex)
        println("the number of vertices are: "+ graph.vertices.count())
        println("the number of edges are: "+ graph.edges.count())

        ////graph calculation
        val ccGraph = graph.connectedComponents()
        val numOfSubGraph = ccGraph.vertices.map{case(_,cc) => cc}.distinct().count()
        println("number of subGraph is: " + numOfSubGraph)

        ////save to hbase
        saveHbase(graph.vertices
          .leftJoin(ccGraph.vertices){
            case(_,(attr,id),oneId) => (attr,id,oneId)
          })

        ////export to csv
        graph.vertices.leftJoin(ccGraph.vertices){
          case(_,(attr,id),oneId) => (attr,id,oneId)
        }.map(x => x._1 + "," + x._2._1 + "," + x._2._2 + "," + x._2._3.get).saveAsTextFile("/yanzi/vertices")

        graph.edges.map(x => x.srcId + "," + x.attr + "," + x.dstId).saveAsTextFile("/yanzi/edges")

        //    test(graph,ccGraph)
        sc.stop()
    }


}

