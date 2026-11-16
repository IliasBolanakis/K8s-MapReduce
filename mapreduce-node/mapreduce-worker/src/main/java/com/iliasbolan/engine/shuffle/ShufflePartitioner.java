package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enterprise-grade {@code ShufflePartitioner} with O(1) memory persistent streams,
 * 256KB LZ4 block framing, and monotonic {@code attemptId} output fencing.
 * <p>
 * <b>Architectural Properties:</b>
 * <ul>
 * <li><b>Zero-Aggregation Write-Through:</b> Streams records directly to partition-specific
 * {@link BufferedWriter} instances backed by a 64KB {@link BufferedOutputStream} and
 * {@link BLOCKSIZE#SIZE_256KB} {@link LZ4FrameOutputStream} frames.</li>
 * <li><b>Eager Directory Allocation:</b> Pre-allocates all partition directories during
 * instantiation so the ESS daemon distinguishes truly empty skewed partitions from missing data.</li>
 * <li><b>Attempt-Fenced Atomic Promotion:</b> Streams stage into
 * {@code <mapTaskId>.attempt.<attemptId>.tmp}. On {@link #close()}, the partitioner inspects
 * {@code <mapTaskId>.attempt}; if a newer attempt ({@code existing > attemptId}) already
 * committed, the stale {@code .tmp} file is discarded without touching {@code <mapTaskId>.lz4}.</li>
 * </ul>
 * </p>
 * <p>
 * <b>Thread Safety:</b><br>
 * Concurrent calls to {@link #appendThreadSafe(List)} synchronize on per-partition lock stripes
 * ({@code partitionLocks[partitionIndex]}). Callers must invoke {@link #close()} after all
 * {@code appendThreadSafe} calls complete.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.4
 * @since 2026-05-15
 */
public class ShufflePartitioner implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ShufflePartitioner.class);

    /** Buffer size (64KB) wrapping the LZ4 output stream for reduced syscall overhead. */
    static final int STREAM_BUFFER_BYTES = 64 * 1024;

    private final String baseShuffleDir;
    private final String jobId;
    private final String mapTaskId;
    private final int numReducers;
    private final int attemptId;
    private final Object[] partitionLocks;

    /** Persistent cache of open writers to prevent LZ4 frame thrashing. */
    private final ConcurrentHashMap<Integer, BufferedWriter> writerCache = new ConcurrentHashMap<>();
    private volatile boolean writeFailed = false;

    /**
     * Initializes the {@code ShufflePartitioner} with default {@code attemptId = 0}.
     *
     * @param baseShuffleDir The root physical directory for ephemeral shuffle data persistence.
     * @param jobId          The universally unique identifier (UUID) of the overarching MapReduce job.
     * @param mapTaskId      The unique identifier for the specific Map task utilizing this partitioner.
     * @param numReducers    The total number of allocated Reducers (used to define partition boundaries).
     * @throws RuntimeException If eager directory allocation fails due to disk access limits or I/O faults.
     */
    public ShufflePartitioner(String baseShuffleDir, String jobId, String mapTaskId, int numReducers) {
        this(baseShuffleDir, jobId, mapTaskId, numReducers, 0);
    }

    /**
     * Initializes the {@code ShufflePartitioner} with explicit {@code attemptId} fencing
     * and eagerly allocates all partition directories.
     *
     * @param baseShuffleDir The root physical directory for ephemeral shuffle data persistence.
     * @param jobId          The universally unique identifier (UUID) of the overarching MapReduce job.
     * @param mapTaskId      The unique identifier for the specific Map task utilizing this partitioner.
     * @param numReducers    The total number of allocated Reducers (used to define partition boundaries).
     * @param attemptId      Monotonic attempt identifier used for staging and zombie-output fencing.
     * @throws RuntimeException If eager directory allocation fails due to disk access limits or I/O faults.
     */
    public ShufflePartitioner(String baseShuffleDir, String jobId, String mapTaskId, int numReducers, int attemptId) {
        this.baseShuffleDir = baseShuffleDir;
        this.jobId = jobId;
        this.mapTaskId = mapTaskId;
        this.numReducers = numReducers;
        this.attemptId = attemptId;

        this.partitionLocks = new Object[numReducers];
        for (int i = 0; i < numReducers; i++) {
            this.partitionLocks[i] = new Object();
            try {
                Files.createDirectories(Paths.get(baseShuffleDir, jobId, String.valueOf(i)));
            } catch (IOException e) {
                logger.error("CRITICAL: Failed to eagerly allocate partition directory for index {}", i, e);
                throw new RuntimeException("Storage initialization failure during partition allocation", e);
            }
        }
    }

    /**
     * Appends a micro-batch of mapped records directly to persistent 256KB-block LZ4 streams.
     *
     * @param buffer A transient, micro-batched collection of {@link KeyValuePair} records.
     * @throws IOException If physical disk writes or stream access operations fail.
     */
    public void appendThreadSafe(List<KeyValuePair> buffer) throws IOException {
        @SuppressWarnings("unchecked")
        List<KeyValuePair>[] groupedByPartition = new List[numReducers];

        for (KeyValuePair pair : buffer) {
            int partitionIndex = (pair.key().hashCode() & Integer.MAX_VALUE) % numReducers;
            if (groupedByPartition[partitionIndex] == null) {
                groupedByPartition[partitionIndex] = new ArrayList<>(buffer.size() / numReducers + 10);
            }
            groupedByPartition[partitionIndex].add(pair);
        }

        for (int i = 0; i < numReducers; i++) {
            List<KeyValuePair> partitionRecords = groupedByPartition[i];
            if (partitionRecords != null && !partitionRecords.isEmpty()) {
                synchronized (partitionLocks[i]) {
                    try {
                        BufferedWriter writer = getOrCreateWriter(i);
                        for (KeyValuePair pair : partitionRecords) {
                            writer.write(pair.key());
                            writer.write('\t');
                            writer.write(pair.value());
                            writer.write('\n');
                        }
                    } catch (Throwable t) {
                        writeFailed = true;
                        throw t;
                    }
                }
            }
        }
    }

    /**
     * Retrieves a cached writer or provisions a new attempt-staged LZ4 stream for the partition.
     *
     * @param partitionIndex The target partition index ({@code 0 .. numReducers - 1}).
     * @return A buffered UTF-8 writer backed by a 64KB buffer and 256KB-block LZ4 frame stream.
     * @throws IOException If creating or opening the staging file fails.
     */
    private BufferedWriter getOrCreateWriter(int partitionIndex) throws IOException {
        BufferedWriter cachedWriter = writerCache.get(partitionIndex);
        if (cachedWriter != null) {
            return cachedWriter;
        }

        Path partitionDir = Paths.get(baseShuffleDir, jobId, String.valueOf(partitionIndex));
        Files.createDirectories(partitionDir);

        Path tmpPath = resolveTmpPath(partitionDir);
        OutputStream os = Files.newOutputStream(tmpPath,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        try {
            BufferedOutputStream bos = new BufferedOutputStream(os, STREAM_BUFFER_BYTES);
            LZ4FrameOutputStream lz4os = new LZ4FrameOutputStream(bos, BLOCKSIZE.SIZE_256KB);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(lz4os, StandardCharsets.UTF_8));
            writerCache.put(partitionIndex, writer);
            return writer;
        } catch (Throwable t) {
            try {
                os.close();
            } catch (IOException ignored) {
            }
            Files.deleteIfExists(tmpPath);
            throw t;
        }
    }

    private Path resolveTmpPath(Path partitionDir) {
        return partitionDir.resolve(mapTaskId + ".attempt." + attemptId + ".tmp");
    }

    /**
     * Safely flushes, closes, and attempt-fences all active LZ4 partition streams.
     * <p>
     * When writes succeeded, checks {@code <mapTaskId>.attempt} in each partition directory:
     * if an existing attempt integer is strictly greater than {@code this.attemptId}, logs a
     * fencing drop warning and deletes the stale {@code .tmp} file without modifying
     * {@code <mapTaskId>.lz4}. Otherwise atomically moves the {@code .tmp} file to
     * {@code <mapTaskId>.lz4} and records {@code this.attemptId} in {@code <mapTaskId>.attempt}.
     * </p>
     */
    @Override
    public void close() {
        logger.info("Finalizing LZ4 partition frames for task: {} (attempt {})", mapTaskId, attemptId);
        for (Map.Entry<Integer, BufferedWriter> entry : writerCache.entrySet()) {
            int partitionIndex = entry.getKey();
            BufferedWriter writer = entry.getValue();
            Path partitionDir = Paths.get(baseShuffleDir, jobId, String.valueOf(partitionIndex));
            Path tmpPath = resolveTmpPath(partitionDir);
            Path finalPath = partitionDir.resolve(mapTaskId + ".lz4");
            Path fencePath = partitionDir.resolve(mapTaskId + ".attempt");
            boolean finalized = false;

            synchronized (partitionLocks[partitionIndex]) {
                try {
                    if (!writeFailed) {
                        writer.flush();
                        writer.close();

                        Integer existingFence = readFenceAttempt(fencePath);
                        if (existingFence != null && existingFence > attemptId) {
                            logger.warn("Fencing drop: partition {} for task {} already finalized by attempt {} (current stale attempt {}); discarding {}",
                                    partitionIndex, mapTaskId, existingFence, attemptId, tmpPath.getFileName());
                        } else {
                            Files.move(tmpPath, finalPath,
                                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                            writeFenceAttempt(partitionDir, fencePath, attemptId);
                            finalized = true;
                        }
                    } else {
                        writer.close();
                    }
                } catch (IOException e) {
                    logger.error("Failed to finalize shuffle stream for task {} partition {} (attempt {})",
                            mapTaskId, partitionIndex, attemptId, e);
                    try {
                        writer.close();
                    } catch (IOException ignored) {
                    }
                } finally {
                    if (!finalized) {
                        try {
                            Files.deleteIfExists(tmpPath);
                        } catch (IOException deleteEx) {
                            logger.warn("Failed to delete temporary shuffle file {}", tmpPath, deleteEx);
                        }
                    }
                }
            }
        }
        writerCache.clear();
    }

    private Integer readFenceAttempt(Path fencePath) {
        if (!Files.exists(fencePath)) {
            return null;
        }
        try {
            String raw = Files.readString(fencePath, StandardCharsets.UTF_8).trim();
            if (raw.isEmpty()) {
                return null;
            }
            return Integer.parseInt(raw);
        } catch (Exception e) {
            logger.warn("Corrupt or unreadable fence file {}; ignoring stale fence marker.", fencePath, e);
            return null;
        }
    }

    private void writeFenceAttempt(Path partitionDir, Path fencePath, int committedAttemptId) throws IOException {
        Path fenceTmp = partitionDir.resolve(mapTaskId + ".attempt." + committedAttemptId + ".fence.tmp");
        try {
            Files.writeString(fenceTmp, String.valueOf(committedAttemptId), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(fenceTmp, fencePath,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(fenceTmp);
        }
    }
}
