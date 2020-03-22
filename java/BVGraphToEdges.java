import it.unimi.dsi.webgraph.*;
import it.unimi.dsi.io.OutputBitStream;
import java.util.Arrays;

public class BVGraphToEdges {

    public static void main(String[] args) throws java.io.IOException {
        String inputPath = args[0];
        String outputPath = args[1];
        ImmutableGraph graph = BVGraph.loadOffline(inputPath);
        NodeIterator nodes = graph.nodeIterator();
        long[] encoded = new long[(int) graph.numArcs()];

        System.out.print("Encoding the values... ");
        int pos = 0;
        long start = System.currentTimeMillis();
        while (nodes.hasNext()) {
            int u = nodes.nextInt();
            LazyIntIterator neighs = nodes.successors();
            int outdeg = nodes.outdegree();
            for (int i = 0; i < outdeg; i++) {
                int v = neighs.nextInt();
                encoded[pos++] = zorder(u, v);
                // System.out.println(u + " " + v);
            }
        }
        System.out.println((System.currentTimeMillis() - start) / 1000.0 + "s");

        System.out.print("Sort the encoded values... ");
        start = System.currentTimeMillis();
        Arrays.sort(encoded);
        System.out.println((System.currentTimeMillis() - start) / 1000.0 + "s");
        System.out.print("Compress and write the stream... ");
        start = System.currentTimeMillis();
        OutputBitStream out = new OutputBitStream(outputPath);
        long last = 0;
        for (long x : encoded) {
            long diff = x - last;
            last = x;
            out.writeLongGamma(diff);
        }
        // add at least 65 zeros to mark the end of the stream
        for (int i = 0; i < 66; i++) {
            out.writeBit(false);
        }
        out.close();
        System.out.println((System.currentTimeMillis() - start) / 1000.0 + "s");
        System.out.println("done");
    }

    // static void writeGamma(OutputBitStream out, long x) {
    // int n = 64 - Long.numberOfLeadingZeros(x);
    // for (int i = 0; i < n - 1; i++) {
    // out.writeBit(false);
    // }
    // out.writeLongGamma(x)
    // }

    static long zorder(int x, int y) {
        long z = 0;
        int mask = 1 << 31;
        for (int i = 0; i < 32; i++) {
            if ((x & mask) == 0) {
                z = z << 1;
            } else {
                z = (z << 1) | 1;
            }
            if ((y & mask) == 0) {
                z = z << 1;
            } else {
                z = (z << 1) | 1;
            }
            x = x << 1;
            y = y << 1;
        }

        return z;
    }

}
