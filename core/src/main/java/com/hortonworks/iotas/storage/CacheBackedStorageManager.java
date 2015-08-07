package com.hortonworks.iotas.storage;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.impl.GuavaCache;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by hlouro on 8/7/15.
 */

public class CacheBackedStorageManager implements StorageManager<Storable> {
    private Cache<StorableKey, Storable> cache;
    private StorageManager<Storable> dao;
    ExecutorService executorService = Executors.newFixedThreadPool(5);

    public CacheBackedStorageManager(Cache<StorableKey, Storable> cache) {
        this.cache = cache;
        this.dao = ((GuavaCache)cache).getDao();
    }

    public void add(Storable storable) throws StorageException {
        cache.put(storable.getStorableKey(), storable);
    }

    public Storable remove(StorableKey key) throws StorageException {
        final Storable old = cache.get(key);
        cache.remove(key);
        return old;
    }

    public void addOrUpdate(Storable storable) throws StorageException {;
        try {
            List<Future<Object>> futures = executorService.invokeAll(null);
            for (Future<Object> future : futures) {
                future.
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cache.put(storable.getStorableKey(), storable);
    }

    public Storable get(StorableKey key) throws StorageException {
        return cache.get(key);
    }

    public Collection<Storable> list(String namespace) throws StorageException {
        return dao.list(namespace);
    }

    public void cleanup() throws StorageException {
        cache.clear();
    }

    public Long nextId(String namespace) throws StorageException {
        return dao.nextId(namespace);
    }

    class WriteCallable implements Callable<Storable> {
        private StorageManager<Storable> sm;
        private Storable storable;

        public Storable call() throws Exception {
            sm.add(storable);
            return null;
        }

    }

    class WriteCallableException implements Callable<Storable> {
        private StorageManager<Storable> sm;
        private Storable storable;

        public Storable call() throws Exception {
            sm.addOrUpdate(storable);
            return null;
        }

    }
}
