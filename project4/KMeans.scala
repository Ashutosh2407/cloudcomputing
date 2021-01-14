import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()
  
  def distance(p:Point,po:Point):Double={
    var dist=Math.sqrt((p._1-po._1)*(p._1-po._1)+(p._2-po._2)*(p._2-po._2))
    return dist
  }

  def main(args: Array[ String ]):Unit= {
    /* ... */
    val conf=new SparkConf().setAppName("KMeans")
    val sc=new SparkContext(conf)
    val points=sc.textFile(args(0)).map(line=>{val b=line.split(",")
      (b(0).toDouble,b(1).toDouble)})//POINTS
    centroids = sc.textFile(args(1)).map( line => { val a = line.split(",")
      (a(0).toDouble,a(1).toDouble)}).collect/* read initial centroids from centroids.txt */

    for ( i <- 1 to 5 ){
        
        val cs = sc.broadcast(centroids)
       centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
                         .groupByKey().map{case(c,poin)=>
                           
                       var count=0
                       var Sum_x=0.0
                       var Sum_y=0.0
                       
                       for(p<-poin){
                         count=count+1
                         Sum_x=Sum_x+p._1
                         Sum_y=Sum_y+p._2
                       }
                       var cent_x=Sum_x/count
                       var cent_y=Sum_y/count
                       (cent_x,cent_y)
                       }.collect
  }             
    centroids.foreach(println)
  }
}

