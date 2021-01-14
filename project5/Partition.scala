import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import scala.math.max

object Partition {

  val depth = 6
  def mapper1(item:(Long,Long,List[Long]))= {
   var list = new ListBuffer[(Long,Long)]
   list+=((item._1,item._2))
   if(item._2 > 0) {
    for(x<-item._3)
     list+=((x,item._2)) }
   list
  }
  def mapper2(item:(Long,(Long,(Long,List[Long]))))={
    var cent =  item._2._2._1
    if(item._2._2._1 == (-1).toLong){
        cent = item._2._1
      }
      (item._1,cent,item._2._2._2)
    }
  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Partition_Job")
    val sc = new SparkContext(conf)
    var counter = 0
    var graph = sc.textFile(args(0)).map(line=>{val a=line.split(',')
      var ids:List[Long]=List[Long]()
      var id = 'x'
      for(id<-a){
        ids=ids:+id.toLong
      }
      var cluster = "-1".toLong
      if(counter<5){
         counter=counter+1
         cluster=ids(0)
      }
      (ids(0),cluster,ids.slice(1,ids.length))})
      
    
   for (i <- 1 to depth){
   

   graph = graph.flatMap{mapper1(_)}.reduceByKey(_ max _).join( graph.map(i=>{(i._1,(i._2,i._3))} ) ).map{mapper2(_)}

   }
     graph.map(x=>(x._2,1)).reduceByKey(_+_).collect().foreach(println)

   }
  
}

