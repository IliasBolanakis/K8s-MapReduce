package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ExternalMergeSorter} covering JVM heap overhead accounting,
 * bounded {@code K=64} multi-pass external merge, eager intermediate run deletion, and
 * 256KB LZ4 block framing.
 */
class ExternalMergeSorterTest {

    @TempDir
    Path tempDir;

    @Test
    void testEstimateRecordMemoryBytes_AccountsForJvmOverheadAndUtf16Chars() {
        KeyValuePair pair = new KeyValuePair("alpha", "1234567");
        // 96 + 2 * (5 + 7) = 120 bytes
        assertEquals(120L, ExternalMergeSorter.estimateRecordMemoryBytes(pair));
    }

    @Test
    void testSortReduceAndSpill_MultiPassOver64Runs_DeletesConsumedRunsAndReducesAccurately() throws IOException {
        Path rawDir = tempDir.resolve("reduce_raw");
        Files.createDirectories(rawDir);

        // Write 140 distinct records across 2 raw stream files; with maxChunkSizeBytes = 1 byte,
        // each record spills to its own sorted run -> 140 runs (> K=64), triggering multi-pass merge!
        Path stream0 = rawDir.resolve("grpc_stream_0.lz4");
        Path stream1 = rawDir.resolve("grpc_stream_1.lz4");

        Map<String, Long> expectedSums = new HashMap<>();
        try (OutputStream os0 = Files.newOutputStream(stream0);
             BufferedOutputStream bos0 = new BufferedOutputStream(os0, 64 * 1024);
             LZ4FrameOutputStream lz0 = new LZ4FrameOutputStream(bos0, BLOCKSIZE.SIZE_256KB);
             BufferedWriter w0 = new BufferedWriter(new OutputStreamWriter(lz0, StandardCharsets.UTF_8));
             OutputStream os1 = Files.newOutputStream(stream1);
             BufferedOutputStream bos1 = new BufferedOutputStream(os1, 64 * 1024);
             LZ4FrameOutputStream lz1 = new LZ4FrameOutputStream(bos1, BLOCKSIZE.SIZE_256KB);
             BufferedWriter w1 = new BufferedWriter(new OutputStreamWriter(lz1, StandardCharsets.UTF_8))) {

            for (int i = 0; i < 140; i++) {
                String key = String.format("key-%03d", i % 35);
                long val = (i % 7) + 1L;
                expectedSums.merge(key, val, Long::sum);
                BufferedWriter target = (i % 2 == 0) ? w0 : w1;
                target.write(key + "\t" + val + "\n");
            }
        }

        Reducer sumReducer = (key, values) -> {
            long total = 0L;
            while (values.hasNext()) {
                total += Long.parseLong(values.next());
            }
            return new KeyValuePair(key, String.valueOf(total));
        };

        // Use maxChunkSizeBytes = 100 so even a single record (>= 96 bytes) spills deterministically -> 140 runs (> 64)
        Path finalOutput = ExternalMergeSorter.sortReduceAndSpill(
                rawDir, sumReducer, 500_000, 100L, ExternalMergeSorter.MAX_MERGE_FAN_IN);

        assertTrue(Files.exists(finalOutput));

        // Verify sorted_runs has <= 64 remaining runs (all initial 140 run_*.lz4 files were deleted after pass 1)
        Path runsDir = rawDir.resolve("sorted_runs");
        List<Path> remainingRuns;
        try (Stream<Path> s = Files.list(runsDir)) {
            remainingRuns = s.toList();
        }
        assertTrue(remainingRuns.size() <= ExternalMergeSorter.MAX_MERGE_FAN_IN,
                "Expected <= 64 runs remaining after multi-pass merge, found: " + remainingRuns.size());
        assertTrue(remainingRuns.stream().noneMatch(p -> p.getFileName().toString().startsWith("run_")),
                "All original run_* files must be deleted after merge pass 1");

        // Verify intermediate merged run uses BLOCKSIZE.SIZE_256KB (BD byte bits 6-4 == 0x50)
        Path sampleMergedRun = remainingRuns.get(0);
        byte[] header = Files.readAllBytes(sampleMergedRun);
        assertTrue(header.length >= 7);
        assertEquals(0x50, header[5] & 0x70, "Intermediate merge runs must use BLOCKSIZE.SIZE_256KB");

        // Verify 100% accurate sorted reduction across all 35 grouped keys
        List<String> lines = Files.readAllLines(finalOutput, StandardCharsets.UTF_8);
        assertEquals(expectedSums.size(), lines.size());
        for (int i = 0; i < lines.size(); i++) {
            String[] parts = lines.get(i).split("\t");
            assertEquals(String.format("key-%03d", i), parts[0], "Output keys must be strictly sorted");
            assertEquals(expectedSums.get(parts[0]), Long.parseLong(parts[1]));
        }
    }
}
