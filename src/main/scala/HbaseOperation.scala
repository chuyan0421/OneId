import java.io.IOException

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext, graphx}

object HbaseOperation{

  def saveHbase(vertice:RDD[(Long, (String,String,Option[graphx.VertexId]))]){

    val hbaseData = vertice
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
    val tableName = "dtplatform:ods_id_mapping_di"
    val jobConf:JobConf = new JobConf(configuration,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    new PairRDDFunctions(hbaseData).saveAsHadoopDataset(jobConf)

  }



//  val configuration = HBaseConfiguration.create()
//  configuration.set("hbase.zookeeper.property.clientPort", "2181")
//  configuration.set("hbase.zookeeper.quorum", "localhost")
//
//  val connection = ConnectionFactory.createConnection(configuration)
//  val admin = connection.getAdmin()
//  val table = connection.getTable(TableName.valueOf("ods_id_mapping_di"))
//
//
//  def main(args:Array[String])={
//
//    createTable("ods_id_mapping_di","mapping")
////    insertTable("54ebdf56d24789f4","mapping","session_id","278259831")
////    insertTable("1004549","mapping","login_id","278259831")
////    insertTable("13911598833","mapping","phone","278259831")
////    insertTable("0C:2C:54:9C:E3:98","mapping","mac","278259831")
//
//    val conf = new SparkConf().setMaster("local[*]").setAppName("HbaseTest Application")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//
////    val configuration = HBaseConfiguration.create()
//
//
//    val tableName = "ods_id_mapping_di"
//    val jobConf:JobConf = new JobConf(configuration,this.getClass)
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
//
//    val rawData = List(("47bee08cf232687a","session_id","482580112"), ("18586890313","phone","482580112"))
//    rawData.foreach(println(_))
//    val localData = sc.parallelize(rawData).map(convert)
//    localData.foreach(println(_))
//    new PairRDDFunctions(localData).saveAsHadoopDataset(jobConf)
////    localData.saveAsHadoopDataset(jobConf)
//
//    sc.stop()
//  }
//
//  def createTable(tableName: String,columnFamily:String) ={
//    val tName = TableName.valueOf(tableName)
//    if(!admin.tableExists(tName)){
//      val descriptor = new HTableDescriptor(tName)
//      descriptor.addFamily(new HColumnDescriptor(columnFamily))
//      admin.createTable(descriptor)
//      println("create successful")
//    }
//  }
//
//  def insertTable(rowKey:String,columnFamily:String, column:String, value:String)={
//    val puts = new Put(rowKey.getBytes())
//    puts.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes())
//    table.put(puts)
//    println("insert successful")
//
//  }
//
//  def close() ={
//    if(connection != null){
//      try{
//        connection.close()
//        println("close successful")
//      }catch {
//        case e:IOException => println("close failed")
//      }
//    }
//  }

//  def main(args:Array[String])={
//
//    val conf = new SparkConf().setMaster("local[*]").setAppName("HbaseTest Application")
//    val sc = new SparkContext(conf)
//
//    val hbaseConf = HBaseConfiguration.create()
////    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
//
//    val jobConf:JobConf = new JobConf(hbaseConf,this.getClass)
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"user")
//
//    val rawData = List((1,"lilei",14), (2,"hanmei",18), (3,"someone",38))
//    val localData = sc.parallelize(rawData).map(convert)
//
//    localData.saveAsHadoopDataset(jobConf)
//
//
//  }
//
//  def convert(triple: (String,String,String))={
//    val p = new Put(Bytes.toBytes(triple._1))
//    p.addColumn(Bytes.toBytes("mapping"),Bytes.toBytes(triple._2),Bytes.toBytes(triple._3))
//    (new ImmutableBytesWritable, p)
//
//  }

}
