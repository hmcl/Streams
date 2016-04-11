package com.hortonworks.iotas.cache.redis;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.service.CacheService;
import com.hortonworks.iotas.cache.stats.CacheStats;
import com.hortonworks.iotas.storage.exception.StorageException;
import com.lambdaworks.redis.RedisConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class RedisStringsCache<K,V> implements Cache<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisStringsCache.class);

    private CacheService<K,V> cacheService;

    private RedisConnection<K,V> redisConnection;


    @Override
    public V get(K key) throws StorageException {
        final V val = redisConnection.get(key);
        if (val == null) {
            val = cacheService.load(key);
        }

//        return redisConnection.hget(key);
        return null;
    }

    @Override
    public Map<K, V> getAllPresent(Iterable<? extends K> keys) {
//        redisConnection.get()
        return null;
    }

    @Override
    public void put(K key, V value) {
        redisConnection.set(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void remove(K key) {

    }

    @Override
    public Map<K, V> removeAllPresent(Iterable<? extends K> keys) {
        return null;
    }

    @Override
    public void clear() {
        Collection<? extends K> keys = new ArrayList<>();
        removeAllPresent(keys);
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public CacheStats stats() {
        return null;
    }

    public static class Builder<K,V> {
        private static final long DEFAULT_MAX_BYTES = 10*1024*1024;     // 10 MBs

        public Builder() { }

        private long sizeBytes = DEFAULT_MAX_BYTES;
        private BytesCalculator bytesCalculator;
        private long maxSizeBytes;

        public Builder setMaxSizeBytes(long maxSizeBytes) {
            this.maxSizeBytes = maxSizeBytes;
            return this;
        }

        public Builder setMaxSizeBytesConverter(BytesCalculator bytesCalculator) {
            this.bytesCalculator = bytesCalculator;
            return this;
        }

        public Cache<K,V> build() {
            if (bytesCalculator != null) {
                LOG.debug("Setting ");

            }
            return null;    //TODO
        }
     }




    public interface BytesCalculator<T> {
        /**
         * @param object object that can be used to calculate the number of bytes that should be used to expire the cache
         * @return bytes
         */
        long computeBytes(T object);
    }


}
