import it.unimi.dsi.webgraph.*;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
// import it.unimi.dsi.logging.ProgressLogger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;

public class BVGraphToEdges {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];
        File scratch = new File("/tmp/scratch");

        ImmutableGraph graph = BVGraph.loadOffline(inputPath);
        long totEdges = graph.numArcs();

        if (scratch.isDirectory()) {
            scratch.delete();
        }
        scratch.mkdir();

        // final int sqrtEdgesPerBlock = 640000;

        NodeIterator nodes = graph.nodeIterator();
        System.out.println("Converting " + totEdges + " edges");

        // HashMap<Long, DataOutputStream> outputs = new HashMap<>();
        long[] buffer = new long[100_000_000];
        // long[] buffer = new long[100_000];

        long cnt = 0;
        System.out.println("Encoding the values... ");
        int pos = 0;
        int chunksCount = 0;
        long start = System.currentTimeMillis();
        while (nodes.hasNext()) {
            int u = nodes.nextInt();
            LazyIntIterator neighs = nodes.successors();
            int outdeg = nodes.outdegree();
            for (int i = 0; i < outdeg; i++) {
                int v = neighs.nextInt();
                // if (u != v) { // ignore self loops
                int src = u;
                int dst = v;
                // We consider the graphs symmetric, and we take as canonical the
                // order that results in the upper right triangle of the
                // adjacency matrix.
                if (src < dst) {
                    src = v;
                    dst = u;
                }
                buffer[pos++] = zorder(u, v);
                if (pos >= buffer.length) {
                    writePartial(buffer, pos, new File(scratch, Integer.toString(chunksCount++)));
                    pos = 0;
                }
                // }
                if (cnt++ % 10000000 == 0) {
                    System.out.print((cnt / totEdges * 100) + "%                \r");
                }
            }
        }
        writePartial(buffer, pos, new File(scratch, Integer.toString(chunksCount++)));

        System.out.println("Merge sort the encoded values, writing them compressed... ");
        start = System.currentTimeMillis();
        mergeAndCompress(scratch, outputPath);
        System.out.println("sort and compress " + (System.currentTimeMillis() - start) / 1000.0 + "s");

        // Count the number of edges in the output
        InputBitStream ibs = new InputBitStream(outputPath);
        long check = 0;
        try {
            while (true) {
                ibs.readLongGamma();
                check++;
            }
        } catch (EOFException e) {
            // done
        }
        System.out.println("Check count: " + check);
        assert check == totEdges;
    }

    static void writePartial(long[] buffer, int until, File output) throws IOException {
        System.out.print("Sorting, pushing, and advancing... ");

        long start = System.currentTimeMillis();
        Arrays.sort(buffer, 0, until);
        // DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new
        // FileOutputStream(output)));
        OutputBitStream out = new OutputBitStream(output);
        long last = 0;
        for (int i = 0; i < until; i++) {
            // out.writeLong(buffer[i]);
            long diff = buffer[i] - last;
            assert diff > 0;
            last = buffer[i];
            out.writeLongGamma(diff);
        }
        out.close();
        System.out.println((System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    static void mergeAndCompress(File scratch, String outputPath) throws IOException, FileNotFoundException {
        File mergedir = new File(scratch, "merge");
        mergedir.mkdir();
        ArrayList<File> files = new ArrayList<>(Arrays.asList(scratch.listFiles(f -> f.isFile())));
        ArrayList<File> outputs = new ArrayList<>();

        int cnt = 0;
        while (files.size() > 1) {
            System.out.println("merging " + files.size() + " files");
            outputs.clear();
            Iterator<File> iter = files.iterator();
            while (iter.hasNext()) {
                File a = iter.next();
                if (iter.hasNext()) {
                    File b = iter.next();
                    File out = new File(mergedir, Integer.toString(cnt++));
                    merge(a, b, out);
                    outputs.add(out);
                } else {
                    outputs.add(a);
                }
            }
            files.clear();
            files.addAll(outputs);
            outputs.clear();
        }

        File complete = files.get(0);

        // add at least 65 zeros to mark the end of the stream for the Rust code
        OutputBitStream out = new OutputBitStream(new FileOutputStream(complete, true));
        for (int i = 0; i < 66; i++) {
            out.writeBit(false);
        }
        out.close();

        // Copy to the output path
        complete.renameTo(new File(outputPath));
    }

    static void merge(File aFile, File bFile, File outFile) throws IOException {
        OutputBitStream out = new OutputBitStream(outFile);
        InputBitStream aStream = new InputBitStream(aFile);
        InputBitStream bStream = new InputBitStream(bFile);

        long edgeCount = 0;

        long aLast = 0;
        long bLast = 0;
        long a = aStream.readLongGamma() + aLast;
        aLast = a;
        long b = bStream.readLongGamma() + bLast;
        bLast = b;

        boolean aMore = true;
        boolean bMore = true;

        long last = 0;
        try {
            while (true) {
                if (a < b) {
                    aMore = false;
                    long diff = a - last;
                    assert diff > 0;
                    last = a;
                    out.writeLongGamma(diff);
                    edgeCount++;
                    a = aStream.readLongGamma() + aLast;
                    aLast = a;
                    aMore = true;
                } else if (a > b) {
                    bMore = false;
                    long diff = b - last;
                    assert diff > 0;
                    last = b;
                    out.writeLongGamma(diff);
                    edgeCount++;
                    b = bStream.readLongGamma() + bLast;
                    bLast = b;
                    bMore = true;
                } else {
                    System.err.println("WARNING: duplicate edge!");
                    throw new RuntimeException("duplicate edge");
                }
            }
        } catch (EOFException e) {
            // we finished one of the streams
        }

        // Now exhaust the files
        if (aMore) {
            try {
                while (true) {
                    long diff = a - last;
                    assert diff > 0;
                    last = a;
                    out.writeLongGamma(diff);
                    edgeCount++;
                    a = aStream.readLongGamma() + aLast;
                    aLast = a;
                }
            } catch (EOFException e) {
                // we finished the stream
            }
        }

        if (bMore) {
            try {
                while (bStream.hasNext()) {
                    long diff = b - last;
                    assert diff > 0;
                    last = b;
                    out.writeLongGamma(diff);
                    edgeCount++;
                    b = bStream.readLongGamma() + bLast;
                    bLast = b;
                }
            } catch (EOFException e) {
                // we finished the stream
            }
        }

        aStream.close();
        bStream.close();
        out.close();

        System.out.println("edges merged: " + edgeCount);
    }

    static class DataChunk implements Comparable<DataChunk> {
        private DataInputStream stream;
        private long top;

        DataChunk(File f) throws IOException, FileNotFoundException {
            this.stream = new DataInputStream(new BufferedInputStream(new FileInputStream(f)));
            this.top = stream.readLong();
        }

        boolean advance() throws IOException {
            if (this.stream.available() > 0) {
                this.top = this.stream.readLong();
                return true;
            } else {
                return false;
            }
        }

        public int compareTo(DataChunk other) {
            if (this.top < other.top) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    static long blockEdge(int sqrtEdgesPerBlock, int src, int dst) {
        int cellI = src / sqrtEdgesPerBlock;
        int cellJ = dst / sqrtEdgesPerBlock;
        return zorder(cellI, cellJ);
    }

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
