import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._


object Partition {
  def main ( args: Array[String] ) {
  	val conf = new SparkConf().setAppName("Partition")
  	val sc = new SparkContext(conf)
  	var count=0

  	val edges: RDD[Edge[Long]] = sc.textFile(args(0)).map( line => { val (node, neighbors) = line.split(",").splitAt(1)
    (node(0).toLong,neighbors.toList.map(_.toLong)) } )
    .flatMap( x => x._2.map(y => (x._1, y)))
    .map(nodes => Edge(nodes._1, nodes._2,(-1).toLong))
  	val initialGraph : Graph[Long, Long] = Graph.fromEdges(edges, "Default").mapVertices((id,_)=> {
  		var v = (-1).toLong
  		if(count < 5){
  			count+=1
  			v=id
  		}
  		v
  	})
  	val sssp = initialGraph.pregel((-1).toLong, 5)(
  	(id, att, msg) => math.max(att, msg),
  	triplet=> {
  		if(triplet.dstAttr == (-1).toLong){
  			Iterator((triplet.dstId, triplet.srcAttr))
  		}
  		else{
  			Iterator.empty
  		}
  	},
  	(x,y)=>math.max(x,y))
  	val op = sssp.vertices.map(initialGraph=> (initialGraph._2, 1))
    .reduceByKey(_+_).map(s=> "(" + s._1.toString + "," + s._2.toString + ")").collect()
  	op.foreach(println)

  }
}
