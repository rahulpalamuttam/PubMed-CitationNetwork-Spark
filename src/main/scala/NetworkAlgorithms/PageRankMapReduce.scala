package NetworkAlgorithms
import NetworkComponents.Node
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.{JavaConversions, Map}

/**
 * Created by rahulpalamuttam on 5/21/15.
 */
object PageRankMapReduce {

  def PageRank(nodeRDD: RDD[Node], Alpha: Double, sc: SparkContext): Array[(Node, Double)] = {
    var alpha = Alpha
    var beta: Double = 1.0 - Alpha
    val totalNodes = nodeRDD.count().toDouble;

    /**
     * Sets the teleport weights to be 1 / #nodes
     */
    val teleportSetNodes = nodeRDD.map(n => {
      val nodeweight = n.nodeWeight;
      n.teleportWeight = nodeweight / totalNodes
      n
    })

    /**
     * Normalizes the outlinks based on weight / #outlinks
     */
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

    /**
     * Filter out the leaf nodes, and find their total weight
     */
    var leafs = nodeSizes.filter(p => p._1.outLinks.size() == 0).map(p => p._2)
    var leafsCount = leafs.count()
    var sumOfLeafNodeSizes = if (leafsCount > 0) leafs.reduce((A, B) => A + B) else 0.0

    System.out.println("The leaf node sizes : " + sumOfLeafNodeSizes);

    /**
     * Compute the node sizes determined by an initial flow from teleport
     */
    var flowFromTeleport = nodeSizes.map(p => {
      p._1.size = (alpha + (beta * sumOfLeafNodeSizes)) * p._1.teleportWeight
      p
    })


    do {
      numIterations += 1
      /**
       * contains the outlinks of all nodes and their updated weights.
       * Each outEdge is outputted as (node-ID, weightSize) pairs
       * These pairs are then grouped by node-ID and the weights are summed.
       */
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


      /**
       * update the node list, with the outlink size.
       * sums from the flow from network steps
       */
      val updatedNodes = flowFromTeleport.map(p => {
        val id = p._1.id
        val sum = flowFromNetworkStepsForOutLinkNodes.get(id)
        p._1.size += (if (sum.size > 0.0) sum.get else 0.0)
        p
      })

      /**
       * Normalization
       * 1) Find the sum of all entire graph based on nodeSize.
       * The sum should always be or very close to 1.
       */
      GraphSum = updatedNodes.map(p => p._1.size).reduce((A, B) => A + B)

      //update the difference of the current iteration
      sqdiff_old = sqdiff
      sqdiff = 0.0

      /**
       * Normalize each node by dividing by the sum of the Graph
       */
      val normalizedNodesWithDifferences = updatedNodes.map(p => {
        p._1.size /= GraphSum
        val sqdifference = Math.abs(p._1.size - p._2.toDouble)
        (p._1, p._1.size, sqdifference)
      })

      // update the sqdiff
      sqdiff += normalizedNodesWithDifferences.map(_._3).reduce((A, B) => A + B)
      val updatedNodeList = normalizedNodesWithDifferences.map(p => (p._1, p._2))
      // ReSet the RDD


      /**
       * Recompute the leaf node weights.
       * Filter out the leaf nodes, and find their total weight
       */
      leafs = updatedNodeList.filter(p => p._1.outLinks.size() == 0).map(p => p._2)
      leafsCount = leafs.count()
      sumOfLeafNodeSizes = if (leafsCount > 0) leafs.reduce((A, B) => A + B) else 0.0



      if (sqdiff == sqdiff_old) {
        System.out.println("What happened!!!!!!!!!!!!!!!");
        alpha += 1.0e-10
        beta = 1.0 - alpha
      }

      System.out.println("Iteration: " + numIterations + "  sqdiff=" + sqdiff)

      /**
       * Recomput the node sizes determined by flow from teleport
       */
      flowFromTeleport = updatedNodeList.map(p => {
        p._1.size = (alpha + (beta * sumOfLeafNodeSizes)) * p._1.teleportWeight
        p
      })

    } while ((numIterations < 200) && (sqdiff > 1.0e-15 || numIterations < 50))

    // return the result
    val result = flowFromTeleport.collect();
    result
  }

  // Used to return a list of normalized outlinks for a given node
  def NormalizedTuple(n: Node): Map[String, Double] = {
    val ScalaMap = JavaConversions.mapAsScalaMap(n.outLinks)
    var sum = 0.0
    if (ScalaMap.size > 0) {
      sum = ScalaMap.map(p => p._2).reduce((A, B) => A + B)
    }
    val fraction = if (sum == 0) 1.0 else sum
    val NormalizedNodes = ScalaMap.map(p => (p._1, p._2 / sum))
    NormalizedNodes
  }
}
