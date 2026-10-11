package com.jieli.cop.bench;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateMeterTest {

    @Test
    void countsMarks() {
        RateMeter m = new RateMeter();
        m.mark();
        m.mark(9);
        assertThat(m.total()).isEqualTo(10);
    }

    @Test
    void reportsNonNegativeRate() {
        RateMeter m = new RateMeter();
        m.mark();
        assertThat(m.ratePerSecond()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void rateGrowsWithMarks() {
        RateMeter m = new RateMeter();
        for (int i = 0; i < 1000; i++) {
            m.mark();
        }
        assertThat(m.ratePerSecond()).isGreaterThan(0.0);
    }
}
