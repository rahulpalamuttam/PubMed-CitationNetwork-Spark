import NetworkComponents.Network;
import NetworkComponents.Node;
import NetworkComponents.PajekNetworkCreator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class reads a network in Pajek format, and generates the following metrics for each node and write in an output file:
 * - Degree centrality: the number of incoming links which equals to the citation count.
 * - PageRank: pageRank computed by power method as explained below. implicitly take the effect of citation cascades.
 * - PageRankNormalized: pageRankof each node is a small probability and the sum of all node's pageRank is 1. We just multiply the pageRank of each node with the number of all links in the network.
 * - Betweenness centrality: the number of shortest paths that goes through a node, may indicate the papers between disciplines.
 * - TODO: may be other node metrics...
 * - TODO: also we may compute global network measures/metrics such as:
 * 		  - the average degree (number of links) per node 
 * 		  - ....	
 *
 * Example output:
 * * NodeId | InDegree(CiteCnt) | pageRank             | pageRankNormalized
 * 22530020 |     3             | 9.502959327539258E-8 | 1.939725622196218
 * 22530021 |     1             | 9.629902990542835E-8 | 1.9656371164178017
 *
 */
public class NetworkMeasuresMetrics {

    final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    /**
     * The main method, start point.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        //Get the inputFileNames from user
        if(args.length < 1){
            System.out.println("Call: java org.biocaddie.citation.network.NetworkMeasuresMetrics <network.net> <optional pageRankDampingFactor>");
            System.exit(1);
        }
        String inputFileName = args[0];
        String fileSeparator = System.getProperty("file.separator");
        String pathToFile = inputFileName.substring(0, inputFileName.lastIndexOf(fileSeparator)+1);

        String outFileName = inputFileName.substring(inputFileName.lastIndexOf(fileSeparator)+1, inputFileName.lastIndexOf(".")) + "_metrics.txt";

        System.out.println("Start computing network measures/metrics...");
        Network network = new Network(inputFileName);
        System.out.println("Start Time: " + dateFormat.format(new Date()));

        //Step 1: Read the network in Pajek.net file format
        PajekNetworkCreator.readPajekNetFile(network);

        //Step 2: Compute PageRank of each node within the network
        double alpha = 0.15; // teleportation probability, default value
//        if (args.length > 1)
//            alpha = Double.valueOf(args[1]);
        pageRank(network, alpha);
        //System.out.println("Computing betweenness");
        //Step 3: Compute the Betweenness centrality of each node within the network
        //betweennessCentrality(network);

        //Step 4: Write the measures/metrics of each node to a file // Node Id | CentralityDegree | PageRank | BetweennessCentrality
        writeResultsToFile(network, pathToFile+outFileName);

        System.out.println("End Time  : " + dateFormat.format(new Date()));
        System.out.println("Done!..");
    }

    /**
     * Following the Greedy::eigenvector(void) implementation of InfoMap written in C++ by Martin Rosvall. http://www.tp.umu.se/~rosvall/code.html
     * This method computes the pageRank of each node within the network using the Power method.
     * Power method iterates while((Niterations < 200) && (sqdiff > 1.0e-15 || Niterations < 50))
     * @param network: The network object which contains the nodes, links and linkWeights.
     */
    private static void pageRank(Network network, double alpha) {

        System.out.println ("Starting PageRank computation...");

        //set teleport_weight
        for (Iterator<Map.Entry<String, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
            Node node = iter.next().getValue();
            node.teleportWeight = node.nodeWeight / network.totalNodeWeight;
        }

        //double alpha = 0.15; // teleportation probability, we take it as a parameter, but default 0.15
        double beta = 1.0-alpha; // probability to take normal step
        int Ndanglings = 0;      //number of dangling nodes
        Vector<String> danglings = new Vector<String>(); //keep the id of danglingNodes

        //****initiate
        // Take care of dangling nodes, normalize outLinks, and calculate total teleport weight
        for (Iterator<Map.Entry<String, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {
            Node node = iter.next().getValue();
            if(node.outLinks.isEmpty()){ //&& (node[i]->selfLink <= 0.0)
                danglings.add(node.id);
                Ndanglings++;
            }else{// Normalize the weights
                double sum = 0.0; //double sum = node->selfLink; // Take care of self-links ??
                for (Iterator<Map.Entry<String, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); )
                    sum += iter2.next().getValue();

                //node[i]->selfLink /= sum;
                for (Iterator<Map.Entry<String, Double>> iter3 = node.outLinks.entrySet().iterator(); iter3.hasNext(); ){
                    String nodeId = iter3.next().getKey();
                    node.outLinks.put(nodeId, node.outLinks.get(nodeId) / sum);
                }
            }
        }

        //*********infoMap's eigenvector() function starts from here
        Map<String, Double> size_tmp = new HashMap<String, Double>(); //initialize it with 1.0/numNode
        double initialSize = 1.0 / network.numNode;
        for (Iterator<Map.Entry<String, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); )
            size_tmp.put(iter.next().getKey(), initialSize);

        int Niterations = 0;
        double danglingSize;
        double sqdiff = 1.0;
        double sqdiff_old;
        double sum;

        do{

            // Calculate dangling size
            danglingSize = 0.0;
            for(int i=0;i<Ndanglings;i++){
                danglingSize += size_tmp.get(danglings.get(i));
            }
            System.out.println(danglingSize);
            // Flow from teleportation
            for (Iterator<Map.Entry<String, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
                Node node = iter.next().getValue();
                node.size = (alpha + beta*danglingSize)*node.teleportWeight;
            }

            // Flow from network steps
            for (Iterator<Map.Entry<String, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
                Node node = iter.next().getValue();
                //node[i]->size += beta*node[i]->selfLink*size_tmp[i]; //selflink!!!
                for (Iterator<Map.Entry<String, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ){
                    Map.Entry<String, Double> entry = iter2.next();
                    network.nodeMap.get(entry.getKey()).size += beta*entry.getValue()*size_tmp.get(node.id);
                }
            }

            // Normalize
            sum = 0.0;
            for (Iterator<Map.Entry<String, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
                sum += iter.next().getValue().size;
            }
            System.out.println("Graph sum = " + sum);
            sqdiff_old = sqdiff;
            sqdiff = 0.0;
            for (Iterator<Map.Entry<String, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
                Node node = iter.next().getValue();
                node.size /= sum;
                sqdiff += Math.abs(node.size - size_tmp.get(node.id));
                size_tmp.put(node.id, node.size);
            }
            Niterations++;

            if(sqdiff == sqdiff_old){  //fprintf(stderr,"\n1.0e-10 added to alpha for convergence (precision error)\n");
                alpha += 1.0e-10;
                beta = 1.0-alpha;
            }
            System.out.println ("Iteration: "+Niterations+ "  sqdiff=" + sqdiff);
        }while((Niterations < 200) && (sqdiff > 1.0e-15 || Niterations < 50));

        System.out.println ("PageRank computation done! The error is " + sqdiff + " after " + Niterations + " iterations.");

        danglingSize = 0.0;
        for(int i=0;i<Ndanglings;i++){
            danglingSize += size_tmp.get(danglings.get(i));
        }

    }

    /**
     * This method computes the betweenness centrality of each node within the network.
     * It implements the algorithm of (Ulrik Brandes, A Faster Algorithm for Betweenness Centrality. Journal of Mathematical Sociology 25(2):163-177, 2001.)
     * Also, inspired by the JUNG java implementation of BetweennessCentrality (http://jung.sourceforge.net)
     * As explained in the above paper, it runs in O(nm) and O(nm + n^2 log n) time on unweighted and weighted networks, respectively,
     * where n is the number of nodes and m is the number of links.
     * Do we need to get a parameter which specifies whether the network is weighted or unweighted???
     * For example: our PDB primary citation network (~2M nodes, ~20M links) is unweighted, and its journal citation network is weighted (~10K nodes, 1.3M links).
     * @param network
     */
    private static void betweennessCentrality(Network network) {

        //this map keeps the list of nodes that are changed during an iteration, at the end of each iteration, we only initialize them, not the whole 2 million dataset
        Map<String, Node> changed = new HashMap<String, Node>();
        int changed_cnt = 0; int cnt_node=0;
        for (Iterator<Map.Entry<String, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {
            System.out.println(cnt_node);
            Node s = iter.next().getValue();
            cnt_node++;
            //initialization
            Deque<Node> stack = new ArrayDeque<Node>();
            Queue<Node> queue = new LinkedList<Node>();

            s.numSPs = 1.0;
            s.distance = 0.0;
            queue.add(s);
            changed.put(s.id, s);

            while (!queue.isEmpty()) {
                Node v = queue.remove();
                stack.push(v);

                for (Iterator<Map.Entry<String, Double>> iter2 = v.outLinks.entrySet().iterator(); iter2.hasNext(); ){
                    Node w = network.nodeMap.get(iter2.next().getKey());
                    //path discovery: w found for the first time
                    if (w.distance < 0.0){
                        w.distance = v.distance + 1.0;
                        queue.add(w);
                        changed.put(w.id, w);
                    }
                    //path counting: edge(v,w) on a shortest path?
                    if (w.distance == v.distance+1.0){
                        w.numSPs += v.numSPs;
                        w.predecessors.add(v);
                        changed.put(w.id, w);
                    }
                }
            }

            //accumulation - back-propagation of dependencies
            while (!stack.isEmpty()) {
                Node w = stack.pop();

                for (Iterator<Node> iter3 = w.predecessors.iterator(); iter3.hasNext();) {
                    Node v = iter3.next();
                    v.dependency += (v.numSPs / w.numSPs) * (1.0 + w.dependency);
                    changed.put(v.id, v);
                }

                if (!w.equals(s)) {
                    w.betweennessCentrality += w.dependency;
                }
            }
            changed_cnt += changed.size();
            for (Iterator<Map.Entry<String, Node>> iterInit = changed.entrySet().iterator(); iterInit.hasNext(); ){
                Node nodeInit = iterInit.next().getValue();
                nodeInit.dependency = 0.0;
                nodeInit.numSPs = 0.0;
                nodeInit.distance = -1.0;
                nodeInit.predecessors = new ArrayList<Node>();
            }
            changed.clear();
        }

        System.out.println(changed_cnt);
    }

    /**
     * This method writes the network measures/metrics of each node to an output file.
     * We use double pipe "||" as a delimiter, because single pipe "|" exist in some of the titles.
     * @param network: The network object which contains the metrics of each node.
     * @param outFileName: The name of the output file.
     * @throws IOException
     */
    private static void writeResultsToFile(Network network, String outFileName) throws IOException{

        BufferedWriter out = new BufferedWriter(new FileWriter(new File(outFileName)));
        out.write("* NodeId || InDegree(CiteCnt) || pageRank || pageRankNormalized || betweenness || nodeName" ); out.newLine();
        String sep = " || "; //separator

        for (Iterator it = network.nodeMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry pair = (Map.Entry) it.next();
            Node node = (Node) pair.getValue();
            out.write(node.id + sep + node.inLinks.size() + sep + node.size + sep + node.size * network.numLink + sep + node.betweennessCentrality + sep + node.name);
            out.newLine();
        }
        out.flush();
        out.close();
    }


}



