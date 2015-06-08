import java.io.{PrintWriter, _}

import NetworkAlgorithms._
import NetworkComponents.{Node, PajekNetworkCreator}
import org.apache.spark.{SparkConf, SparkContext, graphx}

import scala.collection.JavaConversions
import scala.collection.mutable._
import scala.io.Source

/**
 * Created by rahul on 5/10/15.
 */
object Main {
  val sep = "||"
  def main(args : Array[String]): Unit = {
    //val network = PajekNetworkCreator.readPajekNetFile()
    //orderRankFile("classic");
    println("Hello World")
    // node List and numLinks
    val javaList = PajekNetworkCreator.readPajekNetFile(args(0));
    println("Done loading and creating the graph!");
    val numLinks = javaList._2
    println(javaList._2)
    val nodeList = JavaConversions.asScalaBuffer(javaList._1)
//    val vertexSet = nodeList.map(node => (node.id.toLong, (node)))
//    val outedgeSet = nodeList.flatMap(node => node.outLinks.map(outNode => outNode._1.toLong).map(l => (node.id.toLong, l)))
    //val inedgeSet = nodeList.flatMap(node => node.inLinks.map(outNode => outNode._1.toLong).map(l => (l, node.id.toLong)))

//    val edgeSet = (outedgeSet).map(tuple => Edge(tuple._1, tuple._2, "edge"))

    val sparkConf = new SparkConf().setAppName("PubMed-CitationNetwork-Spark").setMaster(args(1)).set("spark.default.parallelism", args(2)).set("spark.shuffle.consolidateFiles", "true")
    val sparkContext = new SparkContext(sparkConf)
    val nodeRDD = sparkContext.parallelize(nodeList)
    val results = PageRankMapReduce.PageRank(nodeRDD, 0.15, sparkContext).sortBy(n => n._1.size);
    writeResultsToFile(results, args(0) + "MapReduce.txt", numLinks)

  }

  def writePageRankResultsToFile(nodeList : Array[(graphx.VertexId, Double)], completeList : Buffer[Node], name : String, numLink : Double) : Unit = {
    val pw = new PrintWriter(new File("SortedPageRank" + name))
      pw.write("* NodeId || InDegree(CiteCnt) || pageRank || pageRankNormalized || betweenness || nodeName\n")

        for(node <- nodeList) {
          val nodeVals = completeList.find(p => p.id.toLong.equals(node._1)).toArray
          val pageRankedNormalized = node._2
          val pageRankedUnNormalized = node._2 / numLink
          pw.write(nodeVals(0).id + sep + nodeVals(0).inLinks.size + sep + pageRankedUnNormalized + sep + pageRankedNormalized + sep + nodeVals(0).betweennessCentrality + sep + nodeVals(0).name +"\n")
        }

      pw.close()
  }

  def writeBetweenessResultsToFile(nodeList : Array[(graphx.VertexId, Int)], completeList : Buffer[Node], name : String, numLink : Double) : Unit = {
    println("writing file")
    val pw = new PrintWriter(new File("SortedBetweeness" + name))
    pw.write("* NodeId || InDegree(CiteCnt) || pageRank || pageRankNormalized || betweenness || nodeName\n")

    for(node <- nodeList) {
      val nodeVals = completeList.find(p => p.id.toLong.equals(node._1)).toArray
      pw.write(nodeVals(0).id + sep + nodeVals(0).inLinks.size + sep + 0 + sep + 0 + sep + node._2 + sep + nodeVals(0).name +"\n")
    }

    pw.close()
  }

  def orderRankFile(name : String): Unit = {
    val lines = Source.fromFile("BetweennessTestResults\\vertices5_metrics_Betweenness.txt").mkString.split("\n")
    lines(0) = "\n"
    val sorted = lines.filter(s => !s.equals("\n")).sortBy(s => s.split("\\|\\|")(4).trim.toDouble)
    val pw = new PrintWriter("Test_Sorted_" + name + ".csv");
    pw.write("* NodeId || InDegree(CiteCnt) || pageRank || pageRankNormalized || betweenness || nodeName")
    for (line <- sorted) {
      pw.write(line + "\n");
    }
    pw.close()
  }

  def writeResultsToFile(result : Array[(Node, Double)], outFilename : String, numLink : Double) : Unit = {
    val pw = new PrintWriter(new File(outFilename))
    pw.write("* NodeId || InDegree(CiteCnt) || pageRank || pageRankNormalized || betweenness || nodeName\n")
    result.map(node => {
      pw.write(node._1.id + sep + node._1.inLinks.size() + sep + node._2 + sep + node._2 * numLink + sep + node._1.betweennessCentrality + sep + node._1.name + "\n")
      //pw.flush()
    })
    pw.close()
  }
}
