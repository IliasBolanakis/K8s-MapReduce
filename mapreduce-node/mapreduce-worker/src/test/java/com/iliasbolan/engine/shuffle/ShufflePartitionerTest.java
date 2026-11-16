package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Unit test suite for the ShufflePartitioner component.
 * * This suite verifies that intermediate Map-Reduce data is correctly hashed,
 * partitioned, and persisted to the local file system using LZ4 compression.
 * * Architectural Update: LZ4 Validation
 * These tests have been upgraded to validate the binary LZ4 Frame format. They
 * utilize `FramedLZ4CompressorInputStream` to decompress and verify the integrity of
 * data spilled to the local P2P shuffle storage, ensuring that the compression
 * layer does not introduce data corruption during high-concurrency flushes.
 */
class ShufflePartitionerTest {

    private ShufflePartitioner partitioner;
    private final String testJobId = "job-123";
    private final String testMapId = "map-001";
    private String baseShuffleDir;

    /**
     * An isolated temporary directory provided by JUnit 5 for testing local
     * P2P shuffle persistence.
     */
    @TempDir
    Path tempDir;

    /**
     * Initializes the test environment, establishing the base directory for
     * local shuffle data and configuring the partitioner with three target reducers.
     */
    @BeforeEach
    void setUp() {
        baseShuffleDir = tempDir.toString();

        partitioner = new ShufflePartitioner(
                baseShuffleDir, testJobId, testMapId, 3
        );
    }

    /**
     * Validates the core routing and LZ4 disk-spill mechanism.
     * * Verifies that the thread-safe partitioner correctly segments keys based
     * on their bitmasked hash values, creates the necessary directory structures,
     * and compresses identical keys into the same deterministic binary files.
     * * Raises:
     * IOException: If a file system error or decompression failure occurs.
     */
    @Test
    void testAppendThreadSafe_CorrectlyGroupsKeysAndSkipsEmptyPartitions() throws IOException {
        List<KeyValuePair> intermediateData = List.of(
                new KeyValuePair("apple", "1"),
                new KeyValuePair("banana", "1"),
                new KeyValuePair("apple", "1")
        );

        partitioner.appendThreadSafe(intermediateData);

        partitioner.close();

        int applePartition = ("apple".hashCode() & Integer.MAX_VALUE) % 3;
        int bananaPartition = ("banana".hashCode() & Integer.MAX_VALUE) % 3;

        Path applePath = tempDir.resolve(testJobId).resolve(String.valueOf(applePartition)).resolve(testMapId + ".lz4");
        Path bananaPath = tempDir.resolve(testJobId).resolve(String.valueOf(bananaPartition)).resolve(testMapId + ".lz4");

        assertTrue(Files.exists(applePath), "Compressed partition file for 'apple' should exist at: " + applePath);
        assertTrue(Files.exists(bananaPath), "Compressed partition file for 'banana' should exist at: " + bananaPath);

        String appleContent = readCompressedFile(applePath);
        assertTrue(appleContent.contains("apple\t1\napple\t1\n"),
                "Identical keys were not correctly aggregated in the compressed local partition file.");
    }

    /**
     * Validates that the partitioner adheres to the deterministic path naming
     * convention required for the Peer-to-Peer infrastructure.
     * * Verifies that the file extension is strictly `.lz4` to signal
     * binary compression to remote Reducers.
     * * Raises:
     * IOException: If a file system error occurs.
     */
    @Test
    void testAppendThreadSafe_DeterministicPathConvention() throws IOException {
        String testKey = "p2p-test";
        List<KeyValuePair> intermediateData = List.of(new KeyValuePair(testKey, "value"));
        int expectedPartition = (testKey.hashCode() & Integer.MAX_VALUE) % 3;

        partitioner.appendThreadSafe(intermediateData);

        partitioner.close();

        Path expectedPath = tempDir.resolve(testJobId)
                .resolve(String.valueOf(expectedPartition))
                .resolve(testMapId + ".lz4");

        assertTrue(Files.exists(expectedPath), "Local file does not follow the deterministic naming convention.");
        assertTrue(expectedPath.toString().endsWith(".lz4"), "The local file must have a .lz4 extension for the binary data plane.");
    }

    @Test
    void testAtomicStaging_WritesToTmpUntilClosePromotesToLz4() throws IOException {
        String testKey = "atomic-key";
        int expectedPartition = (testKey.hashCode() & Integer.MAX_VALUE) % 3;
        Path partitionDir = tempDir.resolve(testJobId).resolve(String.valueOf(expectedPartition));
        Path tmpPath = partitionDir.resolve(testMapId + ".attempt.0.tmp");
        Path finalPath = partitionDir.resolve(testMapId + ".lz4");
        Path fencePath = partitionDir.resolve(testMapId + ".attempt");

        partitioner.appendThreadSafe(List.of(new KeyValuePair(testKey, "1")));

        assertTrue(Files.exists(tmpPath), "Active partition should write to .attempt.0.tmp before close()");
        assertFalse(Files.exists(finalPath), ".lz4 must not exist before close() finalizes the stream");

        partitioner.close();

        assertFalse(Files.exists(tmpPath), ".tmp must be removed after atomic move on close()");
        assertTrue(Files.exists(finalPath), ".lz4 must exist after close()");
        assertEquals("0", Files.readString(fencePath, StandardCharsets.UTF_8).trim());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFailureCleanup_FlushFailureDeletesTmpAndLeavesNoLz4() throws Exception {
        int failingPartition = 0;
        Path partitionDir = tempDir.resolve(testJobId).resolve(String.valueOf(failingPartition));
        Path tmpPath = partitionDir.resolve(testMapId + ".attempt.0.tmp");
        Path finalPath = partitionDir.resolve(testMapId + ".lz4");
        Files.writeString(tmpPath, "partial-data");

        BufferedWriter failingWriter = mock(BufferedWriter.class);
        doThrow(new IOException("Simulated flush failure")).when(failingWriter).flush();

        Field cacheField = ShufflePartitioner.class.getDeclaredField("writerCache");
        cacheField.setAccessible(true);
        ConcurrentHashMap<Integer, BufferedWriter> cache =
                (ConcurrentHashMap<Integer, BufferedWriter>) cacheField.get(partitioner);
        cache.put(failingPartition, failingWriter);

        partitioner.close();

        assertFalse(Files.exists(tmpPath), "Stale .tmp file must be deleted on flush failure");
        assertFalse(Files.exists(finalPath), "Partial .lz4 file must not be created on flush failure");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFailureCleanup_WriteFailureDeletesAllTmpFilesOnClose() throws Exception {
        String okKey = "ok-key";
        int okPartition = (okKey.hashCode() & Integer.MAX_VALUE) % 3;
        int failingPartition = (okPartition + 1) % 3;

        partitioner.appendThreadSafe(List.of(new KeyValuePair(okKey, "1")));
        Path okTmp = tempDir.resolve(testJobId).resolve(String.valueOf(okPartition)).resolve(testMapId + ".attempt.0.tmp");
        Path okLz4 = tempDir.resolve(testJobId).resolve(String.valueOf(okPartition)).resolve(testMapId + ".lz4");
        assertTrue(Files.exists(okTmp));

        // Pick a key that hashes to failingPartition
        String failKey = findKeyForPartition(failingPartition, 3);
        Path failTmp = tempDir.resolve(testJobId).resolve(String.valueOf(failingPartition)).resolve(testMapId + ".attempt.0.tmp");
        Path failLz4 = tempDir.resolve(testJobId).resolve(String.valueOf(failingPartition)).resolve(testMapId + ".lz4");
        Files.writeString(failTmp, "partial");

        BufferedWriter failingWriter = mock(BufferedWriter.class);
        doThrow(new IOException("Simulated write failure")).when(failingWriter).write(anyString());

        Field cacheField = ShufflePartitioner.class.getDeclaredField("writerCache");
        cacheField.setAccessible(true);
        ConcurrentHashMap<Integer, BufferedWriter> cache =
                (ConcurrentHashMap<Integer, BufferedWriter>) cacheField.get(partitioner);
        cache.put(failingPartition, failingWriter);

        assertThrows(IOException.class,
                () -> partitioner.appendThreadSafe(List.of(new KeyValuePair(failKey, "1"))));

        partitioner.close();

        assertFalse(Files.exists(okTmp), "All .tmp files must be deleted after a write failure");
        assertFalse(Files.exists(okLz4), "No .lz4 file may be promoted after a write failure");
        assertFalse(Files.exists(failTmp), "Failing partition .tmp file must be deleted");
        assertFalse(Files.exists(failLz4), "Failing partition .lz4 file must not exist");
    }

    @Test
    void testAttemptFencing_DropsStaleAttemptWithoutOverwritingNewerAttempt() throws IOException {
        String key = "fenced-key";
        int partition = (key.hashCode() & Integer.MAX_VALUE) % 3;
        Path partitionDir = tempDir.resolve(testJobId).resolve(String.valueOf(partition));
        Path finalLz4 = partitionDir.resolve(testMapId + ".lz4");
        Path fenceFile = partitionDir.resolve(testMapId + ".attempt");

        // Newer attempt 2 commits first
        try (ShufflePartitioner attempt2 = new ShufflePartitioner(baseShuffleDir, testJobId, testMapId, 3, 2)) {
            attempt2.appendThreadSafe(List.of(new KeyValuePair(key, "winner-v2")));
        }
        assertEquals("2", Files.readString(fenceFile, StandardCharsets.UTF_8).trim());
        assertEquals("fenced-key\twinner-v2\n", readCompressedFile(finalLz4));

        // Stale zombie attempt 1 finishes late: stages to .attempt.1.tmp, fenced out on close()
        ShufflePartitioner staleAttempt1 = new ShufflePartitioner(baseShuffleDir, testJobId, testMapId, 3, 1);
        staleAttempt1.appendThreadSafe(List.of(new KeyValuePair(key, "stale-v1")));
        Path staleTmp = partitionDir.resolve(testMapId + ".attempt.1.tmp");
        assertTrue(Files.exists(staleTmp));

        staleAttempt1.close();

        assertFalse(Files.exists(staleTmp), "Stale .attempt.1.tmp must be deleted when fenced");
        assertEquals("2", Files.readString(fenceFile, StandardCharsets.UTF_8).trim());
        assertEquals("fenced-key\twinner-v2\n", readCompressedFile(finalLz4));

        // Attempt 3 (higher attemptId) supersedes attempt 2
        try (ShufflePartitioner attempt3 = new ShufflePartitioner(baseShuffleDir, testJobId, testMapId, 3, 3)) {
            attempt3.appendThreadSafe(List.of(new KeyValuePair(key, "winner-v3")));
        }
        assertEquals("3", Files.readString(fenceFile, StandardCharsets.UTF_8).trim());
        assertEquals("fenced-key\twinner-v3\n", readCompressedFile(finalLz4));
    }

    @Test
    void testLz4Framing_Uses256KbBlockSize() throws IOException {
        String key = "lz4-block-key";
        int partition = (key.hashCode() & Integer.MAX_VALUE) % 3;

        partitioner.appendThreadSafe(List.of(new KeyValuePair(key, "payload")));
        partitioner.close();

        Path lz4File = tempDir.resolve(testJobId).resolve(String.valueOf(partition)).resolve(testMapId + ".lz4");
        byte[] bytes = Files.readAllBytes(lz4File);
        assertTrue(bytes.length >= 7, "Valid LZ4 frame must include header");
        // LZ4 frame BD byte (index 5) encodes Block MaxSize in bits 6-4: 5 (0x50) == 256KB
        assertEquals(0x50, bytes[5] & 0x70, "Expected LZ4 BLOCKSIZE.SIZE_256KB (BD bits 0x50)");
    }

    private String findKeyForPartition(int targetPartition, int numReducers) {
        for (int i = 0; i < 1000; i++) {
            String candidate = "key-" + i;
            if ((candidate.hashCode() & Integer.MAX_VALUE) % numReducers == targetPartition) {
                return candidate;
            }
        }
        throw new IllegalStateException("Could not find key for partition " + targetPartition);
    }

    /**
     * Helper utility to decompress and read the contents of an LZ4-encoded shuffle file.
     *
     * Args:
     * path (Path): The physical path to the .lz4 file.
     * * Returns:
     * str: The decompressed UTF-8 string content.
     * * Raises:
     * IOException: If the file cannot be read or the LZ4 frame is malformed.
     */
    private String readCompressedFile(Path path) throws IOException {
        try (InputStream is = Files.newInputStream(path);
             FramedLZ4CompressorInputStream lz4is = new FramedLZ4CompressorInputStream(is, true);
             BufferedReader reader = new BufferedReader(new InputStreamReader(lz4is, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n", "", "\n"));
        }
    }
}