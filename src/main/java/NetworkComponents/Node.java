package NetworkComponents;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility class to hold node attributes.
 */
public class Node implements Serializable{

    public String id;
    public String name;
    public double nodeWeight; //initial nodeWeight, if not given, default 1.0
    public Map<String, Double> inLinks = new HashMap<String, Double>();  // neighborNodeId - linkWeight, default linkWeight = 1.0
    public Map<String, Double> outLinks = new HashMap<String, Double>(); // neighborNodeId - linkWeight, default linkWeight = 1.0

    //pageRank related attributes
    public double teleportWeight = 0.0;
    public double size = 0.0; // stationary distribution of the node. is it also pageRank? need to think...

    //betweenness Centrality related attributes
    public double distance = -1.0; //default distance -1
    public double numSPs = 0.0; // number of shortestPaths, default 0
    public double dependency = 0.0; //default 0
    public ArrayList<Node> predecessors = new ArrayList<Node>();
    public double betweennessCentrality = 0.0; //default 0

    //ranking, these are the ranking of the nodes within the network from 1 to n in descending order.
    public int citeCountRank=0;
    public int pageRank=0;
    public int betweennessCentralityRank=0;

    public Node(){}
    public Node(String p_id, String p_name, double p_nodeWeight){
        id = p_id;
        name = p_name;
        nodeWeight = p_nodeWeight;
    }

    public boolean equals(Node node){
        return this.id == node.id;
    }

    public Node setBetweenness(Double dub){
        betweennessCentrality = dub;
        return this;
    }

    public Node setTeleportWeight(Double dub){
        this.teleportWeight = dub;
        return this;
    }

}