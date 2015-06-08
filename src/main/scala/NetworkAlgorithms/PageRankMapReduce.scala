package NetworkAlgorithms
import NetworkComponents.Node
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions
import scala.collection.Map
/**
 * Created by rahulpalamuttam on 5/21/15.
 */
object PageRankMapReduce {

  // Used to return a list of normalized outlinks for a given node
  def NormalizedTuple(n : Node) : Map[String, Double] = {
    val ScalaMap = JavaConversions.mapAsScalaMap(n.outLinks)
    var sum =  0.0
    if(ScalaMap.size > 0){
      sum = ScalaMap.map(p => p._2).reduce((A, B) => A + B)
    }
    val fraction = if (sum == 0) 1.0 else sum
    val NormalizedNodes = ScalaMap.map(p => (p._1, p._2 / sum))
    NormalizedNodes
  }

  def PageRank(nodeRDD : RDD[Node], Alpha : Double, sc : SparkContext) : Array[(Node, Double)] = {
    var alpha = Alpha
    var beta: Double = 1.0 - Alpha
    val totalNodes = nodeRDD.count().toDouble;
    // sets the teleport weight
    val teleportSetNodes = nodeRDD.map(n => {
      val nodeweight = n.nodeWeight;
      n.teleportWeight = nodeweight / totalNodes
      n
    })

    // normalizes the outlines
    val normalizedOutlinks = teleportSetNodes.map(n => {
      val NormalizedNodes = NormalizedTuple(n)
      NormalizedNodes.map(p => n.outLinks.put(p._1, p._2))
      n
    })

    var numIterations = 0;
    // set the initial size of all nodes
    val nodeSizes = normalizedOutlinks.map(n => (n, 1.0 / totalNodes))

    var GraphSum = 0.0
    var sqdiff_old = 0.0;
    var sqdiff = 0.0;

    var leafs = nodeSizes.filter(p => p._1.outLinks.size() == 0).map(p => p._2)
    var leafsCount = leafs.count()
    var sumOfLeafNodeSizes = if(leafsCount > 0) leafs.reduce((A, B) => A + B) else 0.0

    System.out.println("The leaf node sizes : " + sumOfLeafNodeSizes);
    // node, initialSize, id, nodeSize
    var flowFromTeleport = nodeSizes.map(p => {
      p._1.size = (alpha + (beta * sumOfLeafNodeSizes)) * p._1.teleportWeight
      p
    })

    do {
    // contains the outlinks of all nodes and their updated weights, need to sum up the outlinks however
    val flowFromNetworkStepsForOutLinkNodes = flowFromTeleport.flatMap(n => {
      val ScalaOutLinkMap = JavaConversions.mapAsScalaMap(n._1.outLinks)
      val sizeTmp = n._2
      ScalaOutLinkMap.map(outNode => (outNode._1, beta * outNode._2 * sizeTmp))
    }).groupBy(p => p._1).map(p => {
      val id = p._1
      val allOutNodeUpdates = p._2.map(l => l._2)
      val sumUpdates = allOutNodeUpdates.reduce((A, B) => A + B)
      (id, sumUpdates)
    }).collect.toMap


    // update the node list, with the outlink size sums from the flow from network steps
    val updatedNodes = flowFromTeleport.map(p => {
      val id = p._1.id
      val sum = flowFromNetworkStepsForOutLinkNodes.get(id)

      p._1.size += (if (sum.size > 0.0) sum.get else 0.0)
      p
    })

    //normalize
    // find the sum of the entire graph based on nodeSize
    GraphSum = updatedNodes.map(p => p._1.size).reduce((A, B) => A + B)
      //System.out.println("GraphSum = " + GraphSum)
    sqdiff_old = sqdiff
    sqdiff = 0.0
    val normalizedNodesWithDifferences = updatedNodes.map(p => {
      p._1.size /= GraphSum
      val sqdifference = Math.abs(p._1.size - p._2.toDouble)
      (p._1, p._1.size, sqdifference)
    })

    sqdiff += normalizedNodesWithDifferences.map(_._3).reduce((A, B) => A + B)
    val updatedNodeList = normalizedNodesWithDifferences.map(p => (p._1, p._2))
      // ReSet the RDD


      leafs = updatedNodeList.filter(p => p._1.outLinks.size() == 0).map(p => p._2)
      leafsCount = leafs.count()
      sumOfLeafNodeSizes = if(leafsCount > 0) leafs.reduce((A, B) => A + B) else 0.0

      numIterations += 1

    if (sqdiff == sqdiff_old) {
      System.out.println("What happened!!!!!!!!!!!!!!!");
      alpha += 1.0e-10
      beta = 1.0 - alpha
    }

      System.out.println("Iteration: " + numIterations + "  sqdiff=" + sqdiff)

      flowFromTeleport = updatedNodeList.map(p => {
        p._1.size = (alpha + (beta * sumOfLeafNodeSizes)) * p._1.teleportWeight
        p
      })

  } while ((numIterations < 200) && (sqdiff > 1.0e-15 || numIterations < 50))

    // join the unupdated and updates sizes, by grouping by ID
    val result = flowFromTeleport.collect();
    result
  }
}
