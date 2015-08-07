package com.hortonworks.iotas.cache.writer;

import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorageManager;

/**
 * Created by hlouro on 8/7/15.
 */
public class WriteThrough implements WriterStrategy {
    private StorageManager<Storable> dao;

    public WriteThrough(StorageManager<Storable> dao) {
        this.dao = dao;
    }

    public void add(Storable storable) {
        dao.add(storable);
    }

    public void addOrUpdate(Storable storable) {
        dao.addOrUpdate(storable);
    }

}
