import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx.Graph

object Neo4jOperation{
    val verticesDir="/yanzi/vertices"
    val edgesDir = "/yanzi/edges"
    val verticesDesFile = "/opt/neo4j/neo4j-community-3.3.6/import/combineVertices.csv"
    val edgesDesFile = "/opt/neo4j/neo4j-community-3.3.6/import/combineEdges.csv"


    def exportToCsv(graph: Graph[(String,String),String],ccGraph: Graph[graphx.VertexId, String],sc:SparkContext): Unit ={

        val confHadoop = new Configuration()
//        val hdFs=FileSystem.get(sc.hadoopConfiguration)
        val hdFs=FileSystem.get(confHadoop)
        val localFs = FileSystem.getLocal(confHadoop)

        val verticesPath = new Path(verticesDir)
        val verticesPathFile = new Path(verticesDesFile)
        if(hdFs.exists(verticesPath))
            hdFs.delete(verticesPath,true)
        graph.vertices.leftJoin(ccGraph.vertices){
            case(_,(attr,id),oneId) => (attr,id,oneId)
        }.map(x => x._1 + "," + x._2._1 + "," + x._2._2 + "," + x._2._3.get).saveAsTextFile(verticesDir)

        if(localFs.exists(verticesPathFile)){
            localFs.delete(verticesPathFile,true)
        }
        FileUtil.copyMerge(hdFs,verticesPath,localFs,verticesPathFile,false,confHadoop,null)


        val edgesPath = new Path(edgesDir)
        val edgesPathFile = new Path(edgesDesFile)
        if(hdFs.exists(edgesPath))
            hdFs.delete(edgesPath,true)
        graph.edges.map(x => x.srcId + "," + x.attr + "," + x.dstId).saveAsTextFile(edgesDir)
        if(localFs.exists(edgesPathFile)){
            localFs.delete(edgesPathFile,true)
        }
        FileUtil.copyMerge(hdFs,edgesPath,localFs,edgesPathFile,false,confHadoop,null)

    }

    def myCopyMerge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path, conf: Configuration): Unit ={

    }

}