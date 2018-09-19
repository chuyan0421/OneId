import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx.Graph

object Neo4jOperation{
    val verticesPath="/yanzi/vertices"
    val edgePath = "/yanzi/edges"

    def exportToCsv(graph: Graph[(String,String),String],ccGraph: Graph[graphx.VertexId, String],sc:SparkContext): Unit ={

        val fs=FileSystem.get(sc.hadoopConfiguration)

        if(fs.exists(new Path(verticesPath)))
            fs.delete(new Path(verticesPath),true)
        if(fs.exists(new Path(edgePath)))
            fs.delete(new Path(edgePath),true)

        graph.vertices.leftJoin(ccGraph.vertices){
            case(_,(attr,id),oneId) => (attr,id,oneId)
        }.map(x => x._1 + "," + x._2._1 + "," + x._2._2 + "," + x._2._3.get).saveAsTextFile(verticesPath)
        graph.edges.map(x => x.srcId + "," + x.attr + "," + x.dstId).saveAsTextFile(edgePath)

    }

}