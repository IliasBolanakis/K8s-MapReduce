package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.stream.Stream;

/**
 * Enterprise-grade external merge sort utility for distributed data processing.
 * <p>
 * Guarantees bounded heap memory and bounded file-descriptor usage during reduction by:
 * <ul>
 * <li><b>JVM Object-Overhead Accounting:</b> Estimates each buffered {@link KeyValuePair} at
 * {@code 96 + 2 * (key.length() + value.length())} bytes so heap spills trigger accurately
 * before GC thrashing or OOMKills.</li>
 * <li><b>Bounded Multi-Pass Merge ({@code K = 64}):</b> Caps simultaneous open run readers at
 * {@link #MAX_MERGE_FAN_IN} ({@code 64}). When more than {@code K} sorted runs exist on disk,
 * multi-pass intermediate merges fold up to {@code K} runs at a time into new 256KB-block LZ4
 * runs, eagerly deleting consumed input runs after each pass until {@code <= K} runs remain.</li>
 * <li><b>Concatenated LZ4 Frame Ingestion:</b> Uses Apache Commons Compress with
 * {@code decompressConcatenated = true} and skips 0-byte skew files safely.</li>
 * </ul>
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 5.0
 * @since 2026-05-12
 */
public class ExternalMergeSorter {

    private static final Logger logger = LoggerFactory.getLogger(ExternalMergeSorter.class);

    /** Maximum number of records buffered in memory before spilling a sorted run. */
    static final int DEFAULT_CHUNK_RECORD_LIMIT = 500_000;

    /** Maximum estimated JVM heap bytes per buffered chunk before spilling a sorted run (32 MiB). */
    static final long DEFAULT_MAX_CHUNK_SIZE_BYTES = 32L * 1024 * 1024;

    /** Maximum number of simultaneous open run readers allowed in any merge pass. */
    public static final int MAX_MERGE_FAN_IN = 64;

    /** Buffer size (64KB) wrapping intermediate LZ4 run writers. */
    static final int LZ4_BUFFER_SIZE_BYTES = 64 * 1024;

    /** Fixed JVM object + String/List overhead bytes assumed per {@link KeyValuePair}. */
    static final long RECORD_JVM_OVERHEAD_BYTES = 96L;

    /**
     * Orchestrates the complete Spill-to-Disk multi-pass sort and reduce pipeline
     * using production defaults ({@code 500,000} records, {@code 32 MiB} heap accounting,
     * {@code K = 64} fan-in).
     *
     * @param rawDataDir The local directory containing the raw, LZ4-compressed gRPC stream files.
     * @param reducer    The user-defined {@link Reducer} implementation dynamically loaded into the JVM.
     * @return The {@link Path} to the final serialized file containing the reduced output.
     * @throws IOException If a fatal disk I/O failure occurs during merging or resource allocation.
     */
    public static Path sortReduceAndSpill(Path rawDataDir, Reducer reducer) throws IOException {
        return sortReduceAndSpill(
                rawDataDir,
                reducer,
                DEFAULT_CHUNK_RECORD_LIMIT,
                DEFAULT_MAX_CHUNK_SIZE_BYTES,
                MAX_MERGE_FAN_IN
        );
    }

    /**
     * Orchestrates the Spill-to-Disk multi-pass sort and reduce pipeline with explicit
     * spill thresholds and merge fan-in bounds (exposed for deterministic unit testing).
     *
     * @param rawDataDir        The local directory containing raw {@code grpc_stream_*.lz4} files.
     * @param reducer           The user-defined {@link Reducer} implementation.
     * @param chunkRecordLimit  Maximum records per in-memory run chunk.
     * @param maxChunkSizeBytes Maximum estimated JVM heap bytes per in-memory run chunk.
     * @param maxMergeFanIn     Maximum simultaneous open run readers ({@code >= 2}, {@code 64} in production).
     * @return The {@link Path} to {@code final_reduced_output.txt}.
     * @throws IOException              If disk I/O fails during sorting, merging, or reducing.
     * @throws IllegalArgumentException If {@code maxMergeFanIn < 2}, {@code chunkRecordLimit < 1}, or {@code maxChunkSizeBytes < 1}.
     */
    static Path sortReduceAndSpill(Path rawDataDir,
                                   Reducer reducer,
                                   int chunkRecordLimit,
                                   long maxChunkSizeBytes,
                                   int maxMergeFanIn) throws IOException {
        if (maxMergeFanIn < 2) {
            throw new IllegalArgumentException("maxMergeFanIn must be >= 2, got: " + maxMergeFanIn);
        }
        if (chunkRecordLimit < 1 || maxChunkSizeBytes < 1) {
            throw new IllegalArgumentException("Spill thresholds must be positive.");
        }

        logger.info("Initializing LZ4-Aware ExternalMergeSorter on directory: {} (maxFanIn={})",
                rawDataDir, maxMergeFanIn);

        Path runsDir = rawDataDir.resolve("sorted_runs");
        Files.createDirectories(runsDir);

        List<Path> sortedRuns = createSortedRuns(rawDataDir, runsDir, chunkRecordLimit, maxChunkSizeBytes);
        logger.info("Generated {} compressed sorted runs on disk.", sortedRuns.size());

        if (sortedRuns.size() > maxMergeFanIn) {
            sortedRuns = reduceRunsToFanIn(sortedRuns, runsDir, maxMergeFanIn);
        }

        logger.info("Commencing final streaming {}-way merge and reduce.", sortedRuns.size());
        Path finalOutputFile = rawDataDir.resolve("final_reduced_output.txt");
        performNWayMergeAndReduce(sortedRuns, finalOutputFile, reducer);

        return finalOutputFile;
    }

    /**
     * Estimates the JVM heap footprint of a single {@link KeyValuePair}, accounting for
     * object headers, {@link String} metadata, array references, and UTF-16 character storage.
     *
     * @param pair The key-value pair to account for.
     * @return Estimated heap bytes ({@code 96 + 2 * (key.length() + value.length())}).
     */
    static long estimateRecordMemoryBytes(KeyValuePair pair) {
        return RECORD_JVM_OVERHEAD_BYTES + 2L * (pair.key().length() + pair.value().length());
    }

    /**
     * Reads concatenated input files and spills sorted, 256KB-block LZ4-compressed chunks to disk.
     *
     * @param rawDataDir        The directory containing raw gRPC stream files.
     * @param runsDir           The directory where sorted chunk files will be persisted.
     * @param chunkRecordLimit  Maximum records per run chunk.
     * @param maxChunkSizeBytes Maximum estimated JVM memory bytes per run chunk.
     * @return A {@link List} of {@link Path} objects identifying the generated sorted runs.
     * @throws IOException If disk I/O failures occur while reading streams or spilling runs.
     */
    private static List<Path> createSortedRuns(Path rawDataDir,
                                               Path runsDir,
                                               int chunkRecordLimit,
                                               long maxChunkSizeBytes) throws IOException {
        List<Path> sortedRunFiles = new ArrayList<>();
        List<KeyValuePair> currentChunk = new ArrayList<>(Math.min(chunkRecordLimit, 1024));

        long currentChunkSizeBytes = 0L;
        int runCounter = 0;

        try (Stream<Path> rawFiles = Files.list(rawDataDir)) {
            List<Path> filesToProcess = rawFiles
                    .filter(p -> p.getFileName().toString().startsWith("grpc_stream_") && p.toString().endsWith(".lz4"))
                    .sorted()
                    .toList();

            for (Path rawFile : filesToProcess) {
                if (Files.size(rawFile) == 0) {
                    logger.debug("Skipping 0-byte gRPC stream file: {}", rawFile.getFileName());
                    continue;
                }

                try (InputStream is = Files.newInputStream(rawFile);
                     FramedLZ4CompressorInputStream lz4is = new FramedLZ4CompressorInputStream(is, true);
                     BufferedReader reader = new BufferedReader(new InputStreamReader(lz4is, StandardCharsets.UTF_8))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) {
                            continue;
                        }

                        int tabIndex = line.indexOf('\t');
                        if (tabIndex > 0) {
                            String key = line.substring(0, tabIndex);
                            String value = line.substring(tabIndex + 1);
                            KeyValuePair pair = new KeyValuePair(key, value);
                            currentChunk.add(pair);
                            currentChunkSizeBytes += estimateRecordMemoryBytes(pair);
                        }

                        if (currentChunk.size() >= chunkRecordLimit || currentChunkSizeBytes >= maxChunkSizeBytes) {
                            sortedRunFiles.add(sortAndSpillChunk(currentChunk, runsDir, runCounter++));
                            currentChunk.clear();
                            currentChunkSizeBytes = 0L;
                        }
                    }
                }
            }

            if (!currentChunk.isEmpty()) {
                sortedRunFiles.add(sortAndSpillChunk(currentChunk, runsDir, runCounter));
            }
        }
        return sortedRunFiles;
    }

    /**
     * Iteratively folds sorted runs in batches of at most {@code maxMergeFanIn} until
     * {@code <= maxMergeFanIn} runs remain, eagerly deleting consumed input runs after each pass.
     *
     * @param sortedRuns    Initial list of sorted run files on disk.
     * @param runsDir       Working directory for intermediate merged runs.
     * @param maxMergeFanIn Maximum simultaneous open run readers per merge batch.
     * @return The reduced list of {@code <= maxMergeFanIn} sorted run files.
     * @throws IOException If merging or deleting consumed run files fails.
     */
    private static List<Path> reduceRunsToFanIn(List<Path> sortedRuns, Path runsDir, int maxMergeFanIn) throws IOException {
        List<Path> currentRuns = new ArrayList<>(sortedRuns);
        int passNumber = 1;

        while (currentRuns.size() > maxMergeFanIn) {
            List<Path> nextPassRuns = new ArrayList<>();
            int batchIndex = 0;

            for (int i = 0; i < currentRuns.size(); i += maxMergeFanIn) {
                int end = Math.min(i + maxMergeFanIn, currentRuns.size());
                List<Path> batch = currentRuns.subList(i, end);

                if (batch.size() == 1) {
                    nextPassRuns.add(batch.get(0));
                } else {
                    Path mergedRun = runsDir.resolve("merge_pass_" + passNumber + "_batch_" + (batchIndex++) + ".lz4");
                    mergeSortedRunBatch(batch, mergedRun);
                    for (Path consumedRun : batch) {
                        Files.deleteIfExists(consumedRun);
                    }
                    nextPassRuns.add(mergedRun);
                }
            }

            logger.info("External merge pass {} complete: compacted {} runs into {} runs (maxFanIn={}).",
                    passNumber, currentRuns.size(), nextPassRuns.size(), maxMergeFanIn);
            currentRuns = nextPassRuns;
            passNumber++;
        }

        return currentRuns;
    }

    /**
     * Merges a bounded batch of sorted LZ4 run files into a single sorted 256KB-block LZ4 run file.
     *
     * @param batch     The bounded batch of sorted input run files ({@code <= maxMergeFanIn}).
     * @param outputRun Destination path for the merged intermediate run file.
     * @throws IOException If reading input runs or writing the merged run fails.
     */
    private static void mergeSortedRunBatch(List<Path> batch, Path outputRun) throws IOException {
        PriorityQueue<StreamNode> pq = new PriorityQueue<>();
        List<BufferedReader> activeReaders = new ArrayList<>(batch.size());

        try (OutputStream os = Files.newOutputStream(outputRun, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             BufferedOutputStream bos = new BufferedOutputStream(os, LZ4_BUFFER_SIZE_BYTES);
             LZ4FrameOutputStream lz4os = new LZ4FrameOutputStream(bos, BLOCKSIZE.SIZE_256KB);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(lz4os, StandardCharsets.UTF_8))) {

            for (Path runFile : batch) {
                if (Files.size(runFile) == 0) {
                    continue;
                }
                InputStream is = Files.newInputStream(runFile);
                FramedLZ4CompressorInputStream lz4is = new FramedLZ4CompressorInputStream(is, true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(lz4is, StandardCharsets.UTF_8));
                activeReaders.add(reader);

                StreamNode node = new StreamNode(reader);
                if (node.advance()) {
                    pq.add(node);
                }
            }

            while (!pq.isEmpty()) {
                StreamNode minNode = pq.poll();
                writer.write(minNode.currentKey);
                writer.write('\t');
                writer.write(minNode.currentValue);
                writer.write('\n');

                if (minNode.advance()) {
                    pq.add(minNode);
                }
            }
        } finally {
            for (BufferedReader reader : activeReaders) {
                try {
                    reader.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * Sorts an in-memory chunk and persists it to disk using 256KB-block LZ4 frame compression
     * over a 64KB {@link BufferedOutputStream}.
     *
     * @param chunk   The collection of records currently loaded in memory.
     * @param runsDir The destination directory for the sorted file.
     * @param runId   A unique identifier for naming the resulting run file.
     * @return The {@link Path} to the persisted sorted run.
     * @throws IOException If a disk I/O failure occurs during the write operation.
     */
    private static Path sortAndSpillChunk(List<KeyValuePair> chunk, Path runsDir, int runId) throws IOException {
        chunk.sort(Comparator.comparing(KeyValuePair::key));

        Path runFile = runsDir.resolve("run_" + runId + ".lz4");

        try (OutputStream os = Files.newOutputStream(runFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             BufferedOutputStream bos = new BufferedOutputStream(os, LZ4_BUFFER_SIZE_BYTES);
             LZ4FrameOutputStream lz4os = new LZ4FrameOutputStream(bos, BLOCKSIZE.SIZE_256KB);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(lz4os, StandardCharsets.UTF_8))) {

            for (KeyValuePair pair : chunk) {
                writer.write(pair.key());
                writer.write('\t');
                writer.write(pair.value());
                writer.write('\n');
            }
        }
        logger.debug("Spilled compressed sorted run to disk: {} ({} records)", runFile.getFileName(), chunk.size());
        return runFile;
    }

    /**
     * Merges up to {@code K} compressed sorted runs and streams grouped keys lazily to the user-defined {@link Reducer}.
     *
     * @param sortedRuns The bounded collection of compressed sorted run files to be merged.
     * @param outputFile The destination file for the final reduced results.
     * @param reducer    The user-defined aggregation logic.
     * @throws IOException If disk I/O failures occur during merging or reducing.
     */
    private static void performNWayMergeAndReduce(List<Path> sortedRuns, Path outputFile, Reducer reducer) throws IOException {
        PriorityQueue<StreamNode> pq = new PriorityQueue<>();
        List<BufferedReader> activeReaders = new ArrayList<>(sortedRuns.size());

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            for (Path runFile : sortedRuns) {
                if (Files.size(runFile) == 0) {
                    continue;
                }
                InputStream is = Files.newInputStream(runFile);
                FramedLZ4CompressorInputStream lz4is = new FramedLZ4CompressorInputStream(is, true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(lz4is, StandardCharsets.UTF_8));

                activeReaders.add(reader);

                StreamNode node = new StreamNode(reader);
                if (node.advance()) {
                    pq.add(node);
                }
            }

            while (!pq.isEmpty()) {
                String currentGroupingKey = pq.peek().currentKey;
                StreamGroupingIterator lazyIterator = new StreamGroupingIterator(pq, currentGroupingKey);

                executeReduceAndWrite(currentGroupingKey, lazyIterator, reducer, writer);

                while (lazyIterator.hasNext()) {
                    lazyIterator.next();
                }
            }

        } finally {
            for (BufferedReader reader : activeReaders) {
                try {
                    reader.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * Executes the user-defined {@link Reducer} logic and persists the result to the output writer.
     *
     * @param key      The grouping key.
     * @param iterator Lazy-evaluating stream of values for the key.
     * @param reducer  The applied reduction logic.
     * @param writer   The output stream destination.
     * @throws IOException If a serialization error occurs.
     */
    private static void executeReduceAndWrite(String key, Iterator<String> iterator, Reducer reducer, BufferedWriter writer) throws IOException {
        KeyValuePair reducedResult = reducer.reduce(key, iterator);
        if (reducedResult != null) {
            writer.write(reducedResult.key());
            writer.write('\t');
            writer.write(reducedResult.value());
            writer.write('\n');
        }
    }

    /**
     * Recursively purges a local directory and its entire contents.
     *
     * @param directoryToBeDeleted The target directory path to be purged.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void cleanupDirectory(Path directoryToBeDeleted) {
        if (directoryToBeDeleted == null || !Files.exists(directoryToBeDeleted)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(directoryToBeDeleted)) {
            walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
            logger.info("Successfully cleaned up temporary directory: {}", directoryToBeDeleted);
        } catch (IOException e) {
            logger.warn("Failed to clean up temporary directory {}. Space may be leaked.", directoryToBeDeleted, e);
        }
    }

    /**
     * Lazy-evaluating {@link Iterator} pulling values for a single key from the merge {@link PriorityQueue}.
     */
    private record StreamGroupingIterator(PriorityQueue<StreamNode> pq, String currentKey) implements Iterator<String> {

        @Override
        public boolean hasNext() {
            return !pq.isEmpty() && pq.peek().currentKey.equals(currentKey);
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more values for key: " + currentKey);
            }

            StreamNode minNode = pq.poll();
            assert minNode != null;
            String valueToReturn = minNode.currentValue;

            try {
                if (minNode.advance()) {
                    pq.add(minNode);
                }
            } catch (IOException e) {
                throw new RuntimeException("Fatal disk read error during lazy stream evaluation", e);
            }

            return valueToReturn;
        }
    }

    /**
     * Internal cursor representing an active run file stream during a K-way merge.
     */
    private static class StreamNode implements Comparable<StreamNode> {
        String currentKey;
        String currentValue;
        final BufferedReader reader;

        StreamNode(BufferedReader reader) {
            this.reader = reader;
        }

        boolean advance() throws IOException {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                int tabIndex = line.indexOf('\t');
                if (tabIndex > 0) {
                    this.currentKey = line.substring(0, tabIndex);
                    this.currentValue = line.substring(tabIndex + 1);
                    return true;
                }
            }
            return false;
        }

        @Override
        public int compareTo(StreamNode other) {
            return this.currentKey.compareTo(other.currentKey);
        }
    }
}
