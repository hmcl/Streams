package org.apache.streamline.cache.config.expiry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.TimeUnit;

public class Ttl {
    private long duration;
    private TimeUnit unit;
    private long ttlSeconds;

    @JsonCreator
    public Ttl(@JsonProperty("count") long duration,
               @JsonProperty("unit") TimeUnit unit) {
        this.duration = duration;
        this.unit = unit;
        this.ttlSeconds= unit.toSeconds(duration);
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public TimeUnit getTimeUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public long getTtlSeconds() {
        return ttlSeconds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Ttl ttl = (Ttl) o;

        return ttlSeconds == ttl.ttlSeconds;
    }

    @Override
    public int hashCode() {
        return (int) (ttlSeconds ^ (ttlSeconds >>> 32));
    }
}