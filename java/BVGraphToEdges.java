import it.unimi.dsi.webgraph.*;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Properties;

public class BVGraphToEdges {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];
        File scratch = new File("/tmp/scratch");

        if (new File(outputPath).isDirectory()) {
            System.out.println("output file already exists, exiting");
            System.exit(1);
        }

        ImmutableGraph graph = BVGraph.loadOffline(inputPath);
        long totEdges = graph.numArcs();

        long numNodeGroups = 16;
        long nodeGroupLen = nextPower(graph.numNodes()) / numNodeGroups;
        System.out.println("Partitioning nodes in " + numNodeGroups + " groups of " + nodeGroupLen + " nodes each");
        long chunkMaxLen = nodeGroupLen * nodeGroupLen; // take the square of it
        System.out.println("The maximum chunk length is " + chunkMaxLen);

        if (scratch.isDirectory()) {
            deleteDirectory(scratch);
        }
        scratch.mkdir();

        NodeIterator nodes = graph.nodeIterator();
        System.out.println("Converting " + totEdges + " edges");

        long[] buffer = new long[100_000_000];

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
                if (u != v) { // ignore self loops
                    int src = u;
                    int dst = v;
                    // We consider the graphs symmetric, and we take as canonical the
                    // order that results in the upper right triangle of the
                    // adjacency matrix.
                    if (src > dst) {
                        src = v;
                        dst = u;
                    }
                    buffer[pos++] = zorder(src, dst);
                    if (pos >= buffer.length) {
                        writePartial(buffer, pos, new File(scratch, Integer.toString(chunksCount++)));
                        pos = 0;
                    }
                }
                if (cnt++ % 10000000 == 0) {
                    System.out.print((cnt / totEdges * 100) + "%                \r");
                }
            }
        }
        writePartial(buffer, pos, new File(scratch, Integer.toString(chunksCount++)));

        System.out.println("Merge sort the encoded values, writing them compressed... ");
        start = System.currentTimeMillis();
        File mergedFile = mergeAndCompress(scratch);
        System.out.println("sort and compress " + (System.currentTimeMillis() - start) / 1000.0 + "s");

        System.out.println("Split the file in chunks, and check that there are no duplicates");
        start = System.currentTimeMillis();
        splitFiles(mergedFile, new File(outputPath), chunkMaxLen, (int) nodeGroupLen);
        System.out.println("file split " + (System.currentTimeMillis() - start) / 1000.0 + "s");

        try (FileOutputStream fos = new FileOutputStream(new File(outputPath, "metadata.properties"))) {
            Properties metadata = new Properties();
            metadata.setProperty("chunkLength", Long.toString(chunkMaxLen));
            metadata.setProperty("numNodeGroups", Long.toString(numNodeGroups));
            metadata.store(fos, "");
        }

        deleteDirectory(scratch);
    }

    static int nextPower(int x) {
        x--;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        x++;
        return x;
    }

    static void splitFiles(File mergedFile, File outputDir, long chunkMaxLen, int nodeGroupLen) throws IOException {
        InputDifferenceStream input = new InputDifferenceStream(mergedFile);
        outputDir.mkdir();
        long writtenEdges = 0;
        long currentChunk = 0;
        long lastChunkEdges = 0;

        OutputDifferenceStream output = new OutputDifferenceStream(
                new File(outputDir, "part-" + currentChunk + ".bin"));

        try {
            while (true) {
                long z = input.read();

                int[] baseXY = zorderToPair(z);
                int baseX = (int) Math.floor(baseXY[0] / (double) nodeGroupLen);
                int baseY = (int) Math.floor(baseXY[1] / (double) nodeGroupLen);
                long thisChunk = zorder(baseX, baseY);
                assert thisChunk >= currentChunk : "going backward from " + currentChunk + " to " + thisChunk + " ("
                        + Arrays.toString(zorderToPair(currentChunk)) + " to "
                        + Arrays.toString(zorderToPair(thisChunk)) + ")" + " " + Arrays.toString(baseXY);
                if (thisChunk > currentChunk) {
                    currentChunk = thisChunk;
                    output.close();
                    lastChunkEdges = 0;
                    output = new OutputDifferenceStream(new File(outputDir, "part-" + currentChunk + ".bin"));
                }

                output.write(z);
                writtenEdges++;
                lastChunkEdges++;
            }
        } catch (EOFException e) {
            // done
        } finally {
            System.out.println("closing last chunk " + currentChunk);
            output.close();
        }
        System.out.println("num edges written " + writtenEdges);

        input.close();
    }

    static class OutputDifferenceStream {
        OutputBitStream stream;
        long last;

        OutputDifferenceStream(File path) throws IOException {
            this.stream = new OutputBitStream(path);
            this.last = 0;
        }

        void write(long x) throws IOException {
            long diff = x - this.last;
            assert diff > 0 : "non-positive difference!";
            this.last = x;
            this.stream.writeLongGamma(diff);
        }

        void close() throws IOException {
            // Write at least 65 zeros to mark the end of the stream for
            // future readers (Rust relies on this)
            for (int i = 0; i < 66; i++) {
                this.stream.writeBit(false);
            }
            this.stream.close();
        }
    }

    static class InputDifferenceStream {
        InputBitStream stream;
        long last;

        InputDifferenceStream(File path) throws IOException {
            this.stream = new InputBitStream(path);
            this.last = 0;
        }

        long read() throws IOException {
            long diff = this.stream.readLongGamma();
            long x = this.last + diff;
            this.last = x;
            return x;
        }

        void close() throws IOException {
            this.stream.close();
        }
    }

    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    static void writePartial(long[] buffer, int until, File output) throws IOException {
        System.out.print("Sorting, pushing, and advancing... ");

        long start = System.currentTimeMillis();
        Arrays.sort(buffer, 0, until);
        OutputDifferenceStream out = new OutputDifferenceStream(output);
        out.write(buffer[0]);
        for (int i = 1; i < until; i++) {
            if (buffer[i] != buffer[i - 1]) { // remove duplicates
                out.write(buffer[i]);
            }
        }
        out.close();
        System.out.println((System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    static File mergeAndCompress(File scratch) throws IOException, FileNotFoundException {
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

        return files.get(0);
    }

    static void merge(File aFile, File bFile, File outFile) throws IOException {
        OutputDifferenceStream out = new OutputDifferenceStream(outFile);
        InputDifferenceStream aStream = new InputDifferenceStream(aFile);
        InputDifferenceStream bStream = new InputDifferenceStream(bFile);

        long edgeCount = 0;

        long a = aStream.read();
        long b = bStream.read();

        boolean aMore = true;
        boolean bMore = true;

        long last = 0;
        try {
            while (true) {
                if (a < b) {
                    aMore = false;
                    out.write(a);
                    a = aStream.read();
                    aMore = true;
                } else if (a > b) {
                    bMore = false;
                    out.write(b);
                    b = bStream.read();
                    bMore = true;
                } else {
                    // Remove the duplicate edge
                    out.write(a);
                    aMore = false;
                    a = aStream.read();
                    aMore = true;
                    bMore = false;
                    b = bStream.read();
                    bMore = true;
                    edgeCount++;
                }
            }
        } catch (EOFException e) {
            // we finished one of the streams
        }

        // Now exhaust the files
        if (aMore) {
            try {
                while (true) {
                    out.write(a);
                    edgeCount++;
                    a = aStream.read();
                }
            } catch (EOFException e) {
                // we finished the stream
            }
        }

        if (bMore) {
            try {
                while (true) {
                    out.write(b);
                    edgeCount++;
                    b = bStream.read();
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

    // static class DataChunk implements Comparable<DataChunk> {
    // private DataInputStream stream;
    // private long top;

    // DataChunk(File f) throws IOException, FileNotFoundException {
    // this.stream = new DataInputStream(new BufferedInputStream(new
    // FileInputStream(f)));
    // this.top = stream.readLong();
    // }

    // boolean advance() throws IOException {
    // if (this.stream.available() > 0) {
    // this.top = this.stream.readLong();
    // return true;
    // } else {
    // return false;
    // }
    // }

    // public int compareTo(DataChunk other) {
    // if (this.top < other.top) {
    // return -1;
    // } else {
    // return 1;
    // }
    // }
    // }

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

    static int[] zorderToPair(long z) {
        int x = 0;
        int y = 0;

        long xMask = ((long) 1) << 63;
        long yMask = ((long) 1) << 62;

        for (int i = 0; i < 32; i++) {
            if ((z & xMask) == 0) {
                x = x << 1;
            } else {
                x = (x << 1) | 1;
            }
            if ((z & yMask) == 0) {
                y = y << 1;
            } else {
                y = (y << 1) | 1;
            }
            z = z << 2;
        }

        int[] res = new int[2];
        res[0] = x;
        res[1] = y;
        return res;
    }

}
