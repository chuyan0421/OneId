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

object OneId {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("OneId Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

//    val path = "/E:/oneID/oneId_test.txt" // test file path
    //    val path = "/E:/oneID/data.log.2018-08-13.002" //
    val path = "/yanzi/oneId_test.txt"

    val sqlContext = new SQLContext(sc)

    ///数据预处理，删除空值,$开头的节点在sql会报错
    val preData = sc.textFile(path)
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


    val pcOfData = preData
      .filter(line => line.contains("browser")) /// pc埋点数据
      .filter(line => line.contains("PC端商城")) /// PC端商城
    println("number of valid PC data is: " + pcOfData.count())

    val pcSql = sqlContext.read.json(pcOfData)
    pcSql.registerTempTable("pcSql")

    val pcData = sqlContext
      .sql("SELECT properties.user_id, properties.device_id, " +
        "properties.cookie_id, distinct_id FROM pcSql")


    /* vertices initialization  */
    /// android can get mac, iphone can get idfa, distinct_id may be telephone number
    val userId = createVertexFromDataframe(appData,"login_id","login_id")
    val deviceId = createVertexFromDataframe(appData,"device_id","session_id")
    val mac = createVertexFromDataframe(appData,"mac","mac")
    val idfa = createVertexFromDataframe(appData,"idfa","idfa")
    val phone = createVertexFromDataframe(appData,"distinct_id","phone")

    //    val pcUserId = createVertexFromDataframe(pcData,"user_id","user_id")
//    val pcDeviceId = createVertexFromDataframe(pcData,"device_id","session_id")
    //    val pcCookieId = createVertexFromDataframe(pcData,"cookie_id","cookie_id")
    //    val pcDistinctId = createVertexFromDataframe(pcData,"distinct_id","distinct_id")


    /* edge initialization  */
    val  edgeRDD1 = createEdgeFromDataframe(appData,"device_id","login_id","device_login")
    val  edgeRDD2 = createEdgeFromDataframe(appData,"device_id","mac","device_mac")
    val  edgeRDD3 = createEdgeFromDataframe(appData,"device_id","idfa","device_idfa")
    val  edgeRDD4 = createEdgeFromDataframe(appData,"device_id","distinct_id","device_phone")

    //    val  edgeRDD5 = createEdgeFromDataframe(pcData,"device_id","user_id","device_login")
    //    val  edgeRDD6 = createEdgeFromDataframe(pcData,"device_id","cookie_id","device_cookie")
    //    val  edgeRDD7 = createEdgeFromDataframe(pcData,"device_id","distinct_id","device_distinct")


    ///construct  graph
    val vertexRDD = userId.union(deviceId).union(mac).union(idfa).union(phone)
//      .union(pcDeviceId)
    //    verticeRDD.collect().foreach(println(_))
    val relationRDD = edgeRDD1.union(edgeRDD2).union(edgeRDD3).union(edgeRDD4)

    //    println(relationRDD.first())
    val defaultVertex = ("Missing","Missing")
    val graph = Graph(vertexRDD,relationRDD,defaultVertex)
    println("the number of vertices are: "+ graph.vertices.count())
    println("the number of edges are: "+ graph.edges.count())

    ////graph calculation
    val ccGraph = graph.connectedComponents()
    val numOfSubGraph = ccGraph.vertices.map{case(_,cc) => cc}.distinct().count()
    println("number of subGraph is: " + numOfSubGraph)

    val hbaseData = graph.vertices
      .leftJoin(ccGraph.vertices){
        case(_,(attr,id),oneId) => (attr,id,oneId)
      }
//      .filter{case (_,(_,_,c)) => c!=Some(0) } ///空的节点会生成oneID为0
      .map{case(id,(attr,oId,oneId))=>
        val p = new Put(Bytes.toBytes(oId))
        p.addColumn(Bytes.toBytes("mapping"),Bytes.toBytes("original_id"),Bytes.toBytes(oId))
        p.addColumn(Bytes.toBytes("mapping"),Bytes.toBytes("id_type"),Bytes.toBytes(attr))
        p.addColumn(Bytes.toBytes("mapping"),Bytes.toBytes("one_id"),Bytes.toBytes(oneId.get.toString))
        (new ImmutableBytesWritable, p)
      }

    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    configuration.set("hbase.zookeeper.quorum", "localhost")

//    val tableName = "ods_id_mapping_di"
    val tableName = "test"
    val jobConf:JobConf = new JobConf(configuration,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    new PairRDDFunctions(hbaseData).saveAsHadoopDataset(jobConf)

    //    test(graph,ccGraph)
    sc.stop()
  }

  def test(graph: Graph[(String,String),String ], ccGraph: Graph[graphx.VertexId, String]) ={
    outPutDisplay(graph,ccGraph,"13911598833")//phone android
    outPutDisplay(graph,ccGraph,"78:36:CC:59:47:AD")///session_id android
    outPutDisplay(graph,ccGraph,"50799EFF-E113-4017-B54B-6A2513C6F444")//
    outPutDisplay(graph,ccGraph,"13506650288")///
    outPutDisplay(graph,ccGraph,"13408442244")//
    outPutDisplay(graph,ccGraph,"165312b063a87-077dcfdb92435a-36465d60-1310720-165312b063bb5")///

    // test
    val searchTest = "13911598833"  //phone
    println()
    println("subId: " + searchTest + " , oneId is: ")
    val (oneId, vertexName) = findOneId(graph,ccGraph,searchTest)
    println(oneId.toString + " " + vertexName)

    val searchTest2 = 562296217L
    println()
    println("OneId: " + searchTest2 + " , relevant ids are:")
    val relevantIds2 = relevatIdsFromOneId(graph,ccGraph,searchTest2)
    for(a <- 0 until relevantIds2.size){
      println(searchTest2.toString + " " + relevantIds2(a))
    }

    /////查找子图中入度>=2的节点
    //    ccGraph.inDegrees.filter(t => t._2 >= 2).foreach(println(_))
    //    val searchTest3 = 1867229706
    //    val temp = graph.vertices.lookup(searchTest3)(0)
    //    println(temp)
    //    outPutDisplay(graph,ccGraph,temp._2)///

  }


}

