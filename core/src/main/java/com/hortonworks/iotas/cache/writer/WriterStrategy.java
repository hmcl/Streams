package com.hortonworks.iotas.cache.writer;

import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorageManager;

/**
 * Created by hlouro on 8/7/15.
 */
public interface WriterStrategy {
    void add(Storable storable);

    void addOrUpdate(Storable storable);
}
