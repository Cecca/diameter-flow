import it.unimi.dsi.webgraph.*;

public class BVGraphToEdges {

    public static void main(String[] args) throws java.io.IOException {
        String inputPath = args[0];
        ImmutableGraph graph = BVGraph.loadOffline(inputPath);
        NodeIterator nodes = graph.nodeIterator();
        while (nodes.hasNext()) {
            int u = nodes.nextInt();
            LazyIntIterator neighs = nodes.successors();
            int outdeg = nodes.outdegree();
            for (int i = 0; i < outdeg; i++) {
                int v = neighs.nextInt();
                System.out.println(u + " " + v);
                System.out.println(v + " " + u);
            }
        }
    }
}
