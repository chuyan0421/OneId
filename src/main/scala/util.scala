import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer

object util {

  //BKDRHash 哈希算法根据点值，返回唯一的long型id
  def genBKDRHash2(str: String): Long = {
    val seed: Int = 131 // 31 131 1313 13131 131313 etc..
    var hash: Int = 0
    var i :Int = 0
    while (i < str.length) {
      hash = hash * seed + str.charAt(i)
      hash = hash & 0x7FFFFFFF
      //println(hash)
      i+=1
    }
    return hash.toLong
  }
  ///创建顶点
  def createVertexFromDataframe(rawData:DataFrame,colName: String,aliasName:String):RDD[(Long, (String,String))] = {
    val vertexRDD = rawData.select(colName).rdd.distinct()
      .map(x =>(genBKDRHash2(x.getString(0)), (aliasName,x.getString(0)) ))
      .filter(x => x._1 != 0) ///空的节点产生的oneId为 0
    return vertexRDD

  }

  ///创建边
  def createEdgeFromDataframe(rawData:DataFrame,srcName:String,destName:String, attrName:String):RDD[Edge[String]] = {
    val edgeRDD = rawData.select(srcName,destName)
      .rdd.distinct().filter(_(1)!= "")
      .map(x => Edge(genBKDRHash2(x.getString(0)),genBKDRHash2(x.getString(1)),attrName))
      .filter(x => x.srcId != 0)
      .filter(x => x.dstId != 0)
    return edgeRDD
  }

  ///找到某个id对应的oneId
  def findOneId(graph: Graph[(String,String),String ], ccGraph: Graph[graphx.VertexId, String],string: String):(Long,(String, String)) ={
    val oneId = ccGraph.vertices.lookup(genBKDRHash2(string))(0)
    val vertexName = graph.vertices.lookup(genBKDRHash2(string))(0)
    return (oneId,vertexName)
  }

  ////通过oneId返回相应的子id
  def relevatIdsFromOneId(graph: Graph[(String,String),String],ccGraph: Graph[graphx.VertexId, String],oneId: Long):ArrayBuffer[(String,String)]={
    val relevantHashId = ccGraph.vertices.filter(v => v._2 ==oneId).map(v => v._1).collect()
    var relevantIds = ArrayBuffer[(String,String)]()
    for(a <- 0 until relevantHashId.size){
      relevantIds += graph.vertices.lookup(relevantHashId(a))(0)
    }
    return relevantIds
  }

  def outPutDisplay(graph: Graph[(String,String),String],ccGraph: Graph[graphx.VertexId, String],string: String):Null={
    println()
    println("subId: " + string + " , relevant ids are:")
    val (oneId, _) = findOneId(graph,ccGraph,string)
    val relevantIds = relevatIdsFromOneId(graph,ccGraph,oneId)
    for(a <- 0 until relevantIds.size){
      println(oneId.toString + " " + relevantIds(a))
    }
    return null

  }

  def test(graph: Graph[(String,String),String ], ccGraph: Graph[graphx.VertexId, String]) = {
    outPutDisplay(graph, ccGraph, "13911598833") //phone android
    outPutDisplay(graph, ccGraph, "78:36:CC:59:47:AD") ///session_id android
    outPutDisplay(graph, ccGraph, "50799EFF-E113-4017-B54B-6A2513C6F444") //
    outPutDisplay(graph, ccGraph, "13506650288") ///
    outPutDisplay(graph, ccGraph, "13408442244") //
    outPutDisplay(graph, ccGraph, "165312b063a87-077dcfdb92435a-36465d60-1310720-165312b063bb5") ///

    // test
    val searchTest = "13911598833" //phone
    println()
    println("subId: " + searchTest + " , oneId is: ")
    val (oneId, vertexName) = findOneId(graph, ccGraph, searchTest)
    println(oneId.toString + " " + vertexName)

    val searchTest2 = 562296217L
    println()
    println("OneId: " + searchTest2 + " , relevant ids are:")
    val relevantIds2 = relevatIdsFromOneId(graph, ccGraph, searchTest2)
    for (a <- 0 until relevantIds2.size) {
      println(searchTest2.toString + " " + relevantIds2(a))
    }
  }

  ////从cc子图出查询oneID及原图顶点属性
  //
  //  def oneIdFromGraph(graph: Graph[(String,String),String],ccGraph: Graph[graphx.VertexId, String],string: String):(Long,ArrayBuffer[(String,String)]) = {
  //    val oneId = ccGraph.vertices.lookup(genBKDRHash2(string))(0)
  //    val relevantHashId = ccGraph.vertices.filter(v => v._2 ==oneId).map(v => v._1).collect()
  //    var relevantIds = ArrayBuffer[(String,String)]()
  //    for(a <- 0 until relevantHashId.size){
  //      relevantIds += graph.vertices.lookup(relevantHashId(a))(0)
  //    }
  //    return (oneId,relevantIds)
  //  }

  /////查找子图中入度>=2的节点
  //    ccGraph.inDegrees.filter(t => t._2 >= 2).foreach(println(_))
  //    val searchTest3 = 1867229706
  //    val temp = graph.vertices.lookup(searchTest3)(0)
  //    println(temp)
  //    outPutDisplay(graph,ccGraph,temp._2)///



}
