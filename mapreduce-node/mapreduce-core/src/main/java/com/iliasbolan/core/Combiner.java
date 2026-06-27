package com.iliasbolan.core;

import java.util.Iterator;

/**
 * The core contract for the optional local-reduction phase of a Map-Reduce job.
 * <p>
 * A Combiner acts as a localized optimization engine that executes strictly on
 * the Map node before data is spilled to disk or shuffled across the network.
 * Its primary architectural purpose is to minimize network I/O bandwidth and
 * protect the downstream Reducers from Out-Of-Memory (OOM) Data Skew crashes.
 * </p>
 * <p>
 * <b>Semantic Boundary:</b><br>
 * While structurally similar to a {@link Reducer}, a Combiner operates under strict
 * mathematical constraints: its logic must be both <i>commutative</i> and <i>associative</i>.
 * Because a Combiner only processes a partial, localized subset of data, it cannot
 * perform holistic operations (e.g., calculating a final average or a global median).
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.0
 * @since 2026-06-27
 */
public interface Combiner {

    /**
     * Compresses a partial, localized stream of intermediate values for a specific key.
     *
     * @param key    The intermediate key shared by all values in the micro-batch.
     * @param values An {@link Iterator} of {@link String} values intercepted directly
     * from the Mapper's execution context.
     * @return A single {@link KeyValuePair} representing the highly compressed
     * local aggregation, or {@code null} if no output should be emitted.
     */
    KeyValuePair combine(String key, Iterator<String> values);
}