package NetworkComponents;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility class to hold network attributes:
 */
public class Network implements Serializable {

    public String name;  //network file name
    public int numNode;  //number of nodes
    public int numLink;  //number of links
    public double totalNodeWeight = 0.0;

    public Map<String, Node> nodeMap = new HashMap<String, Node>(); //key:nodeId value:nodeName

    public Network() {
    }

    public Network(String p_name) {
        name = p_name;
    }
}