package com.hortonworks.iotas.storage;


import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

//TODO: The synchronization is broken right now, so all the methods dont guarantee the semantics as described in the interface.
public class InMemoryStorageManager implements StorageManager<Storable> {

    private ConcurrentHashMap<String, ConcurrentHashMap<PrimaryKey, Storable>> storageMap =  new ConcurrentHashMap<String, ConcurrentHashMap<PrimaryKey, Storable>>();
    private ConcurrentHashMap<String, Long> sequenceMap = new ConcurrentHashMap<String, Long>();

    public void add(Storable storable) throws AlreadyExistsException {
        final Storable existing = get(storable.getStorableKey());

        if(existing == null) {
            addOrUpdate(storable);
        } else if (existing.equals(storable)) {
            return;
        } else {
            throw new AlreadyExistsException("Another instnace with same id = " + storable.getPrimaryKey()
                    + " exists with different value in namespace " + storable.getNameSpace()
                    + " Consider using addOrUpdate method if you always want to overwrite.");
        }
    }

    public Storable remove(StorableKey key) {
        if(storageMap.containsKey(key.getNameSpace())) {
            return storageMap.get(key.getNameSpace()).remove(key.getPrimaryKey());
        }
        return null;
    }

    public void addOrUpdate(Storable storable) {
        String namespace = storable.getNameSpace();
        PrimaryKey id = storable.getPrimaryKey();
        if(!storageMap.containsKey(namespace)) {
            storageMap.putIfAbsent(namespace, new ConcurrentHashMap<PrimaryKey, Storable>());
        }
        storageMap.get(namespace).put(id, storable);
    }

    public Storable get(StorableKey key) throws StorageException {
        return storageMap.containsKey(key.getNameSpace()) ? storageMap.get(key.getNameSpace()).get(key.getPrimaryKey()) : null;
    }

    public Collection<Storable> list(String namespace) throws StorageException {
        return storageMap.containsKey(namespace) ? storageMap.get(namespace).values() : new LinkedList<Storable>();
    }

    public void cleanup() throws StorageException {
        //no-op
    }

    public Long nextId(String namespace){
        Long id = this.sequenceMap.get(namespace);
        if(id == null){
            id = 0l;
        }
        id++;
        this.sequenceMap.put(namespace, id);
        return id;
    }
}
