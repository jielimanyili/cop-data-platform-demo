package com.jieli.cop.bench;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe events-per-second meter. Cumulative average over its lifetime — simple,
 * deterministic, and sufficient for a bounded benchmark run.
 */
public final class RateMeter {

    private final AtomicLong count = new AtomicLong();
    private final long startNanos = System.nanoTime();

    public void mark() {
        count.incrementAndGet();
    }

    public void mark(long n) {
        count.addAndGet(n);
    }

    public long total() {
        return count.get();
    }

    public double elapsedSeconds() {
        return (System.nanoTime() - startNanos) / 1_000_000_000.0;
    }

    public double ratePerSecond() {
        double seconds = elapsedSeconds();
        return seconds <= 0 ? 0.0 : count.get() / seconds;
    }
}
