package org.apache.streamline.cache.config.eviction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.streamline.cache.config.jackson.BytesUnit;

public class Size {
    private long count;
    private long bytes;
    private BytesUnit unit;

    @JsonCreator
    public Size(@JsonProperty("count") long count,
                @JsonProperty("unit") BytesUnit unit) {
        this.count = count;
        this.unit = unit;
        this.bytes = unit.toBytes(count);
    }

    public long getCount() {
        return count;
    }

    public long getBytes() {
        return bytes;
    }

    public BytesUnit getUnit() {
        return unit;
    }
}
