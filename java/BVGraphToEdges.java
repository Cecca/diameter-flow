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

        long chunkMaxLen = graph.numNodes() / 2;
        chunkMaxLen *= chunkMaxLen; // take the square of it
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
        splitFiles(mergedFile, new File(outputPath), chunkMaxLen);
        System.out.println("file split " + (System.currentTimeMillis() - start) / 1000.0 + "s");

        try (FileOutputStream fos = new FileOutputStream(new File(outputPath, "metadata.properties"))) {
            Properties metadata = new Properties();
            metadata.setProperty("chunkLength", Long.toString(chunkMaxLen));
            metadata.store(fos, "");
        }

        deleteDirectory(scratch);
    }

    static void splitFiles(File mergedFile, File outputDir, long chunkMaxLen) throws IOException {
        // TODO: write metadata
        InputBitStream ibs = new InputBitStream(mergedFile);
        outputDir.mkdir();
        long writtenEdges = 0;
        long lastRead = 0;
        long lastWritten = 0;
        long currentChunkBaseZ = 0;
        long currentChunk = 0;
        long lastChunkEdges = 0;

        OutputBitStream output = new OutputBitStream(new File(outputDir, "part-" + currentChunk + ".bin"));

        try {
            while (true) {
                long diff = ibs.readLongGamma();
                assert diff != 0 : "zero diff";
                long z = lastRead + diff;
                lastRead = z;
                if (z > currentChunkBaseZ + chunkMaxLen) {
                    writeClosingZeros(output);
                    while (z > currentChunkBaseZ + chunkMaxLen) {
                        currentChunkBaseZ += chunkMaxLen;
                        currentChunk += 1;
                    }
                    System.out.println("Closed chunk with " + lastChunkEdges + " edges");
                    lastChunkEdges = 0;
                    lastWritten = 0;
                    System.out.println("New chunk (" + currentChunk + ") with base z coordinate " + currentChunkBaseZ
                            + " because z=" + z + " " + Arrays.toString(zorderToPair(z)) + ", " + writtenEdges
                            + " edges written so far");
                    output = new OutputBitStream(new File(outputDir, "part-" + currentChunk + ".bin"));
                }
                long writeDiff = z - lastWritten;
                lastWritten = z;
                output.writeLongGamma(z);
                writtenEdges++;
                lastChunkEdges++;
            }
        } catch (EOFException e) {
            // done
        } finally {
            System.out.println("closing last chunk");
            writeClosingZeros(output);
        }
        System.out.println("num edges written " + writtenEdges);

        ibs.close();
    }

    // Write at least 65 zeros to mark the end of the stream for
    // future readers (Rust relies on this)
    static void writeClosingZeros(OutputBitStream out) throws IOException {
        for (int i = 0; i < 66; i++) {
            out.writeBit(false);
        }
        out.close();
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
        OutputBitStream out = new OutputBitStream(output);
        long last = 0;
        for (int i = 0; i < until; i++) {
            long diff = buffer[i] - last;
            // If diff == 0 then we have a duplicate edge, which we remove
            if (diff != 0) {
                assert diff > 0;
                last = buffer[i];
                out.writeLongGamma(diff);
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
                    last = a;
                    if (diff != 0) {
                        assert diff > 0;
                        out.writeLongGamma(diff);
                        edgeCount++;
                    }
                    a = aStream.readLongGamma() + aLast;
                    aLast = a;
                    aMore = true;
                } else if (a > b) {
                    bMore = false;
                    long diff = b - last;
                    last = b;
                    if (diff != 0) {
                        assert diff > 0;
                        out.writeLongGamma(diff);
                        edgeCount++;
                    }
                    b = bStream.readLongGamma() + bLast;
                    bLast = b;
                    bMore = true;
                } else {
                    // Remove the duplicate edge
                    bMore = false;
                    aMore = false;
                    long diff = a - last;
                    last = a;
                    out.writeLongGamma(diff);
                    edgeCount++;
                    a = aStream.readLongGamma() + aLast;
                    aLast = a;
                    aMore = true;
                    b = bStream.readLongGamma() + bLast;
                    bLast = b;
                    bMore = true;
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
