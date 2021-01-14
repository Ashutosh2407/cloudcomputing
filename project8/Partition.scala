import org.apache.spark.graphx.{Graph=>Graph1,VertexId,Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Partition {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Project8")
    val sc = new SparkContext(conf)
    
    val graphedges: RDD[Edge[Long]]=sc.textFile(args(0)).map(line => { val (node, adjacent) = line.split(",").splitAt(1)
          (node(0).toLong, adjacent.toList.map(_.toLong))}).flatMap(x => x._2.map(y => (x._1,y)))
          .map(nodes => Edge(nodes._1,nodes._2,(-1).toLong))
          
          var c = 0
          
          val graph: Graph1[Long, Long] = Graph1.fromEdges(graphedges, "defaultProperty").mapVertices((id, _) =>{
          var n = (-1).toLong
          if(c<5){
             c+=1
             n = id
          }
          n})
   
    val conn = graph.pregel((-1).toLong, 6)(
      (id, oldCluster, newCluster) => math.max(oldCluster, newCluster),
      triplet=>{
        if(triplet.dstAttr==(-1).toLong){
          Iterator((triplet.dstId,triplet.srcAttr))
        }
        else{
          Iterator.empty
        }
      }
      ,(a, b) => math.max(a,b))
      val output_result = conn.vertices.map(graph => (graph._2, 1))
                                  .reduceByKey(_ + _).sortByKey()
                                  .map(k =>k._1.toString+" "+k._2.toString)
    println("Output computed components graph: ");
    output_result.collect().foreach(println)

  }
}

