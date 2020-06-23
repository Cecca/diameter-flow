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
import it.unimi.dsi.logging.ProgressLogger;

public class BVGraphToEdges {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];
        File scratch = new File(new File(outputPath).getParent(), "scratch");

        if (new File(outputPath).isDirectory()) {
            System.out.println("output file already exists, exiting");
            System.exit(1);
        }

        ImmutableGraph graph = BVGraph.loadOffline(inputPath);
        long totEdges = graph.numArcs();
        ProgressLogger pl = new ProgressLogger();
        pl.expectedUpdates = totEdges;

        // long numNodeGroups = 16;
        // long nodeGroupLen = nextPower(graph.numNodes()) / numNodeGroups;
        // System.out.println("Partitioning nodes in " + numNodeGroups + " groups of " +
        // nodeGroupLen + " nodes each");
        // long chunkMaxLen = nodeGroupLen * nodeGroupLen; // take the square of it
        // System.out.println("The maximum chunk length is " + chunkMaxLen);
        long chunkLen = 100000;

        if (scratch.isDirectory()) {
            deleteDirectory(scratch);
        }
        scratch.mkdir();

        NodeIterator nodes = graph.nodeIterator();
        System.out.println("Converting " + totEdges + " edges");

        long[] buffer = new long[100_000_000];

        long cntSelfLoops = 0;
        long cntDuplicates = 0;

        int maxId = 0;

        long cnt = 0;
        System.out.println("Encoding the values... ");
        pl.start();
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
                    maxId = (src > maxId)? src : maxId;
                    maxId = (dst > maxId)? dst : maxId;
                    // We consider the graphs symmetric, and we take as canonical the
                    // order that results in the upper right triangle of the
                    // adjacency matrix.
                    if (src > dst) {
                        src = v;
                        dst = u;
                    }
                    buffer[pos++] = zorder(src, dst);
                    if (pos >= buffer.length) {
                        cntDuplicates += writePartial(buffer, pos, new File(scratch, Integer.toString(chunksCount++)));
                        pos = 0;
                    }
                } else {
                    cntSelfLoops++;
                }
                pl.lightUpdate();
            }
        }
        cntDuplicates += writePartial(buffer, pos, new File(scratch, Integer.toString(chunksCount++)));
        pl.stop();
        System.out.println("Ignored " + cntSelfLoops + " self loops and " + cntDuplicates + " duplicates");

        System.out.println("Merge sort the encoded values, writing them compressed... ");
        start = System.currentTimeMillis();
        File mergedFile = mergeAndCompress(scratch);
        System.out.println("sort and compress " + (System.currentTimeMillis() - start) / 1000.0 + "s");

        Matrix arrangement = new Matrix(32, maxId + 1);

        System.out.println("Split the file in chunks, and check that there are no duplicates");
        start = System.currentTimeMillis();
        splitFiles(mergedFile, new File(outputPath), arrangement);
        System.out.println("file split " + (System.currentTimeMillis() - start) / 1000.0 + "s");

        arrangement.save(new File(outputPath));

        // try (FileOutputStream fos = new FileOutputStream(new File(outputPath,
        // "metadata.properties"))) {
        // Properties metadata = new Properties();
        // metadata.setProperty("chunkLength", Long.toString(chunkMaxLen));
        // metadata.setProperty("numNodeGroups", Long.toString(numNodeGroups));
        // metadata.store(fos, "");
        // }

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

    static void splitFiles(File mergedFile, File outputDir, Matrix arrangement) throws IOException {
        InputDifferenceStream input = new InputDifferenceStream(mergedFile);
        outputDir.mkdir();
        long writtenEdges = 0;

        int nBlocks = arrangement.blocksPerSide * arrangement.blocksPerSide;
        OutputDifferenceStream[] writers = new OutputDifferenceStream[nBlocks];
        try {
            for (int i=0; i<nBlocks; i++) {
                writers[i] = new OutputDifferenceStream(new File(outputDir, "part-" + i + ".bin"));
            }

            while (true) {
                long z = input.read();
                int[] xy = zorderToPair(z);
                writers[arrangement.rowMajorBlock(xy[0], xy[1])].write(z);
                writtenEdges++;
            }
        } catch (EOFException e) {
            // done
        } finally {
            for (int i=0; i<nBlocks; i++) {
                writers[i].close();
            }
        }
        System.out.println("num edges written " + writtenEdges);

        input.close();
    }

    static class Matrix {
        int blocksPerSide;
        int elemsPerBlock;

        Matrix(int blocksPerSide, int sideElements) {
            int elemsPerBlock = (int) Math.ceil((sideElements / (double) blocksPerSide));
            this.blocksPerSide = blocksPerSide;
            this.elemsPerBlock = elemsPerBlock;
        }

        public int rowMajorBlock(int x, int y) {
            // The index within a block
            int inner_x = x % this.elemsPerBlock;
            int inner_y = y % this.elemsPerBlock;
            // The index of the (square) block
            int block_x = x / this.elemsPerBlock;
            int block_y = y / this.elemsPerBlock;

            if (inner_x < inner_y) {
                // Upper triangle
                return block_x * this.blocksPerSide + block_y;
            } else {
                // Lower triangle
                return block_y * this.blocksPerSide + block_x;
            }
        }

        public void save(File output) throws IOException {
            try (FileOutputStream fos = new FileOutputStream(new File(output, "arrangement.txt"))) {
                Properties metadata = new Properties();
                metadata.setProperty("blocks_per_side", Long.toString(this.blocksPerSide));
                metadata.setProperty("elems_per_block", Long.toString(this.elemsPerBlock));
                metadata.store(fos, "");
            }
        }
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
            if (diff == 0) {
              System.err.println("x="+x+ " last=" +this.last);
            }
            assert diff != 0 : "difference is zero!";
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
            assert diff > 0 : "Read a zero difference!";
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

    static long writePartial(long[] buffer, int until, File output) throws IOException {
        System.out.print("Sorting, pushing, and advancing ("
            + output + ")... ");

        long cntDuplicates = 0;

        long start = System.currentTimeMillis();
        Arrays.sort(buffer, 0, until);
        OutputDifferenceStream out = new OutputDifferenceStream(output);
        out.write(buffer[0]);
        for (int i = 1; i < until; i++) {
            if (buffer[i] != buffer[i - 1]) { // remove duplicates
                out.write(buffer[i]);
            } else {
                cntDuplicates++;
            }
        }
        out.close();
        System.out.println((System.currentTimeMillis() - start) / 1000.0 + "s");
        return cntDuplicates;
    }

    static File mergeAndCompress(File scratch) throws IOException, FileNotFoundException {
        File mergedir = new File(scratch, "merge");
        mergedir.mkdir();
        ArrayList<File> files = new ArrayList<>(Arrays.asList(scratch.listFiles(f -> f.isFile())));
        ArrayList<File> outputs = new ArrayList<>();

        long cntDuplicates = 0;
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
                    cntDuplicates += merge(a, b, out);
                    outputs.add(out);
                    a.delete();
                    b.delete();
                } else {
                    outputs.add(a);
                }
            }
            files.clear();
            files.addAll(outputs);
            outputs.clear();
        }
        System.out.println("Ignored further " + cntDuplicates + " when merging");

        return files.get(0);
    }

    static long merge(File aFile, File bFile, File outFile) throws IOException {
        OutputDifferenceStream out = new OutputDifferenceStream(outFile);
        InputDifferenceStream aStream = new InputDifferenceStream(aFile);
        InputDifferenceStream bStream = new InputDifferenceStream(bFile);

        long cntDuplicates = 0;
        long edgeCount = 0;

        long a = aStream.read();
        long b = bStream.read();

        boolean aMore = true;
        boolean bMore = true;

        System.out.println(">> Merging the two files");
        long last = 0;
        try {
            while (true) {
                if (a < b) {
                    aMore = false;
                    out.write(a);
                    last = a;
                    a = aStream.read();
                    edgeCount++;
                    aMore = true;
                } else if (a > b) {
                    bMore = false;
                    out.write(b);
                    last = b;
                    b = bStream.read();
                    edgeCount++;
                    bMore = true;
                } else {
                    // Remove the duplicate edge
                    out.write(a);
                    last = a;
                    aMore = false;
                    a = aStream.read();
                    aMore = true;
                    bMore = false;
                    b = bStream.read();
                    bMore = true;
                    edgeCount++;
                    cntDuplicates++;
                }
            }
        } catch (EOFException e) {
            // we finished one of the streams
        }

        // Now exhaust the files
        System.out.println("   Exhausing left file");
        if (aMore) {
            try {
                while (true) {
                    if (a > last) {
                      out.write(a);
                      last = a;
                      edgeCount++;
                    }
                    a = aStream.read();
                }
            } catch (EOFException e) {
                // we finished the stream
            }
        }

        System.out.println("   Exhausing rigth file");
        if (bMore) {
            try {
                while (true) {
                    if (b > last) {
                      out.write(b);
                      last = b;
                      edgeCount++;
                    }
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
        return cntDuplicates;
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
