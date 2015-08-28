package com.hortonworks.iotas.storage;

import com.hortonworks.iotas.storage.impl.memory.InMemoryStorageManager;

public class InMemoryStorageManagerTest extends AbstractStoreManagerTest {
    private StorageManager storageManager = new InMemoryStorageManager();

    @Override
    public StorageManager getStorageManager() {
        return storageManager;
    }
}
