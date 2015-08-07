package com.hortonworks.iotas.cache.writer;

import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorageManager;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hlouro on 8/7/15.
 */
public class WriteBehind implements WriterStrategy {
    private static final int NUM_THREADS = 5;
    private StorageManager<Storable> dao;
    private ExecutorService executorService;

    public WriteBehind(StorageManager<Storable> dao) {
        this(dao, Executors.newFixedThreadPool(NUM_THREADS));
    }

    public WriteBehind(StorageManager<Storable> dao, ExecutorService executorService) {
        this.dao = dao;
        this.executorService = executorService;
    }

    public void add(Storable storable) {
        executorService.submit(new AddCallable(storable));
    }

    public void addOrUpdate(Storable storable) {

    }

    private class AddCallable implements Callable<Storable> {
        private Storable storable;

        public AddCallable(Storable storable) {
            this.storable = storable;
        }

        public Storable call() throws Exception {
            dao.add(storable);
            return null;    //TODO since not returning value, perhaps we can use runnable
        }
    }

    class AddOrUpdateCallable implements Callable<Storable> {
        private Storable storable;

        public AddOrUpdateCallable(Storable storable) {
            this.storable = storable;
        }

        public Storable call() throws Exception {
            dao.addOrUpdate(storable);
            return null;    //TODO since not returning value, perhaps we can use runnable
        }

    }

}
