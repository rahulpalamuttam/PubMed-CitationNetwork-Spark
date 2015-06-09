package NetworkComponents;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by rahul on 5/10/15.
 */
public class PajekNetworkCreator {

    final static Charset ENCODING = StandardCharsets.UTF_8;

    /**
     * Following the implementation of InfoMap written in C++ (by Martin Rosvall):
     * This method reads network in Pajek format
     * each directed link occurring only once, and link weights > 0.
     * For more information, see http://vlado.fmf.uni-lj.si/pub/networks/pajek/.
     * Node weights are optional and sets the relative proportion to which
     * each node receives teleporting random walkers. Default value is 1.
     * Example network with three nodes and four directed and weighted links:
     * *Vertices 3
     * 1 "Name of first node" 1.0
     * 2 "Name of second node" 2.0
     * 3 "Name of third node" 1.0
     * *Arcs 4
     * 1 2 1.0
     * 1 3 1.7
     * 2 3 2.0
     * 3 2 1.2
     *
     * @param network: network object which contains the inputFileName
     * @throws java.io.IOException
     */
    public static void readPajekNetFile(Network network) throws IOException {

        BufferedReader reader = Files.newBufferedReader(Paths.get(network.name), ENCODING);
        String line = null;
        String currentPart = "";
        int linkCount = 0;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.equals("")) //skip the empty lines, if there is any
                continue;

            if (line.length() > 9 && line.substring(0, 9).equalsIgnoreCase("*Vertices")) {
                network.numNode = new Integer(line.substring(line.indexOf(" ") + 1)).intValue();
                currentPart = "nodes";
                continue;
            }

            if (line.length() > 6 && (line.substring(0, 6).equalsIgnoreCase("*Edges") || line.substring(0, 5).equalsIgnoreCase("*Arcs"))) {
                network.numLink = new Integer(line.substring(line.indexOf(" ") + 1)).intValue();
                currentPart = "links";
                continue;
            }

            //Reading nodes
            if (currentPart.equals("nodes")) {
                int nameStart = line.indexOf("\"");
                int nameEnd = line.lastIndexOf("\"");

                String id = line.substring(0, nameStart - 1);
                String name = line.substring(nameStart + 1, nameEnd);
                String nodeWeight_str = line.substring(nameEnd + 1).trim();
                Double nodeWeight = 1.0; //default value
                if (nodeWeight_str.length() > 0 && Double.valueOf(nodeWeight_str) > 0.0)
                    nodeWeight = Double.valueOf(nodeWeight_str);

                Node node = new Node(id, name, nodeWeight);
                network.nodeMap.put(id, node);
                network.totalNodeWeight += nodeWeight;
            }

            //Reading links
            if (currentPart.equals("links")) {
                linkCount++;

                String[] tokens = line.split(" ");
                String from = tokens[0].trim();
                String to = tokens[1].trim();
                Double weight = 1.0; //default value
                if (tokens.length > 2 && tokens[2].trim().length() > 0 && Double.valueOf(tokens[2].trim()) > 0.0)
                    weight = Double.valueOf(tokens[2].trim());

                //add link to both ends
                network.nodeMap.get(from).outLinks.put(to, weight);
                network.nodeMap.get(to).inLinks.put(from, weight);
            }

        }

        if (network.numNode != network.nodeMap.size()) {
            System.out.println("Number of nodes not matching, exiting!!!");
            System.exit(1);
        }
        if (network.numLink != linkCount) {
            System.out.println("Number of links not matching, exiting!!!");
            System.exit(1);
        }
    }

    public static Tuple2<List<Node>, Double> readPajekNetFile(String fileName) throws IOException {
        Network network = new Network();
        BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), ENCODING);
        String line = null;
        String currentPart = "";
        int linkCount = 0;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.equals("")) //skip the empty lines, if there is any
                continue;

            if (line.length() > 9 && line.substring(0, 9).equalsIgnoreCase("*Vertices")) {
                network.numNode = new Integer(line.substring(line.indexOf(" ") + 1)).intValue();
                currentPart = "nodes";
                continue;
            }

            if (line.length() > 6 && (line.substring(0, 6).equalsIgnoreCase("*Edges") || line.substring(0, 5).equalsIgnoreCase("*Arcs"))) {
                network.numLink = new Integer(line.substring(line.indexOf(" ") + 1)).intValue();
                currentPart = "links";
                continue;
            }

            //Reading nodes
            if (currentPart.equals("nodes")) {
                int nameStart = line.indexOf("\"");
                int nameEnd = line.lastIndexOf("\"");

                String id = line.substring(0, nameStart - 1);
                String name = line.substring(nameStart + 1, nameEnd);
                String nodeWeight_str = line.substring(nameEnd + 1).trim();
                Double nodeWeight = 1.0; //default value
                if (nodeWeight_str.length() > 0 && Double.valueOf(nodeWeight_str) > 0.0)
                    nodeWeight = Double.valueOf(nodeWeight_str);

                Node node = new Node(id, name, nodeWeight);
                network.nodeMap.put(id, node);
                network.totalNodeWeight += nodeWeight;
            }

            //Reading links
            if (currentPart.equals("links")) {
                linkCount++;

                String[] tokens = line.split(" ");
                String from = tokens[0].trim();
                String to = tokens[1].trim();
                Double weight = 1.0; //default value
                if (tokens.length > 2 && tokens[2].trim().length() > 0 && Double.valueOf(tokens[2].trim()) > 0.0)
                    weight = Double.valueOf(tokens[2].trim());

                //add link to both ends
                network.nodeMap.get(from).outLinks.put(to, weight);
                network.nodeMap.get(to).inLinks.put(from, weight);
            }

        }

        if (network.numNode != network.nodeMap.size()) {
            System.out.println("Number of nodes not matching, exiting!!!");
            System.exit(1);
        }
        if (network.numLink != linkCount) {
            System.out.println("Number of links not matching, exiting!!!");
            System.exit(1);
        }
        List<Node> temp = new ArrayList<>();
        Iterator t = network.nodeMap.entrySet().iterator();
        while (t.hasNext()) {
            Map.Entry nex = (Map.Entry) t.next();
            Node next = (Node) nex.getValue();
            temp.add(next);
        }
        Double val = network.numLink * 1.0;
        return new Tuple2<>(temp, val);
    }


}
