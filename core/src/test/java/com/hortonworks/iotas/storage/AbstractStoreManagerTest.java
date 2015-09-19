package com.hortonworks.iotas.storage;

import com.hortonworks.iotas.catalog.DataFeed;
import com.hortonworks.iotas.catalog.DataSource;
import com.hortonworks.iotas.catalog.Device;
import com.hortonworks.iotas.catalog.ParserInfo;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.service.CatalogService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractStoreManagerTest {
    protected static final Logger log = LoggerFactory.getLogger(AbstractStoreManagerTest.class);

    //NOTE: If you are adding a new entity, create a list of 4 items where the 1st and 2nd item has same value for primary key.
    //and then add this list to storables variable defined below.

    protected List<StorableTest> storableTests;

    @Before
    public void setup() {
        setStorableTests();
    }

    protected abstract void setStorableTests();

    protected class StorableTest {
        protected List<Storable> storableList;

        /**
         * Performs any initialization steps that are required to test this storable instance, for example,
         * initialize the a parent table that a child table refers to (e.g. DataSource and Device)
         */
        public void init() {

        }

        protected void addStorables(List<Storable> storables) {
            for (Storable storable : storables) {
                getStorageManager().addOrUpdate(storable);
            }
        }

        public void test() {
            final Storable storable1 = storableList.get(0);
            final Storable storable2 = storableList.get(1);
            final Storable storable3 = storableList.get(2);
            final Storable storable4 = storableList.get(3);
            String namespace = storable1.getNameSpace();

            Assert.assertNull(getStorageManager().get(storable1.getStorableKey()));

            //test add by inserting the first item in list.
            getStorageManager().add(storable1);
            Assert.assertEquals(storable1, getStorageManager().get(storable1.getStorableKey()));

            //test update by calling addOrUpdate on second item which should have the same primary key value as first item.
            getStorageManager().addOrUpdate(storable2);
            Assert.assertEquals(storable2, getStorageManager().get(storable1.getStorableKey()));

            //add 3rd item, only added so list operation will return more then one item.
            getStorageManager().addOrUpdate(storable3);
            Assert.assertEquals(storable3, getStorageManager().get(storable3.getStorableKey()));

            //test remove by adding 4th item and removing it.
            getStorageManager().addOrUpdate(storable4);
            Assert.assertEquals(storable4, getStorageManager().get(storable4.getStorableKey()));
            Storable removed = getStorageManager().remove(storable4.getStorableKey());
            Assert.assertNull(getStorageManager().get(storable4.getStorableKey()));
            // check that the correct remoted item got returned
            Assert.assertEquals(storable4, removed);

            //Test list method. The final state of storage layer should have the 2nd item (updated version of 1st item) and 3rd Item.
            final Set<Storable> expected = new HashSet<Storable>() {{
                add(storable2);
                add(storable3);
            }};
            Assert.assertEquals(expected, new HashSet(getStorageManager().list(storable2.getStorableKey().getNameSpace())));

            //Test method with query parameters(filter) matching only the item storable3
            Collection<Storable> found = getStorageManager().find(namespace, buildQueryParamsForPrimaryKey(storable3));
            Assert.assertEquals(found.size(), 1);
            Assert.assertTrue(found.contains(storable3));
        }

        public void close() {
            getStorageManager().cleanup();
        }

        public List<Storable> getStorableList() {
            return storableList;
        }

        List<CatalogService.QueryParam> buildQueryParamsForPrimaryKey(Storable storable) {
            final Map<Schema.Field, Object> fieldsToVal = storable.getPrimaryKey().getFieldsToVal();
            final List<CatalogService.QueryParam> queryParams = new ArrayList<>(fieldsToVal.size());

            for (Schema.Field field : fieldsToVal.keySet()) {
                CatalogService.QueryParam qp = new CatalogService.QueryParam(field.getName(), fieldsToVal.get(field).toString());
                queryParams.add(qp);
            }

            return queryParams;
        }
    }

    public class ParsersTest extends StorableTest {
        {
            storableList = new ArrayList<Storable>() {{
                add(createParserInfo(1l, "parser-1"));
                add(createParserInfo(1l, "parser-2"));
                add(createParserInfo(2l, "parser-3"));
                add(createParserInfo(3l, "parser-4"));
            }};
        }

        protected ParserInfo createParserInfo(Long id, String name) {
            ParserInfo pi = new ParserInfo();
            pi.setParserId(id);
            pi.setParserName(name);
            pi.setClassName("com.org.apache.TestParser");
            pi.setJarStoragePath("/tmp/parser.jar");
            pi.setParserSchema(new Schema.SchemaBuilder().fields(new Schema.Field("deviceId", Schema.Type.LONG), new Schema.Field("deviceName", Schema.Type.STRING)).build());
            pi.setVersion(0l);
            pi.setTimestamp(System.currentTimeMillis());
            return pi;
        }
    }


    public class DataFeedsTest extends StorableTest {
        {
            storableList = new ArrayList<Storable>() {{
                add(createDataFeed(1l, "feed-1"));
                add(createDataFeed(1l, "feed-2"));
                add(createDataFeed(2l, "feed-3"));
                add(createDataFeed(3l, "feed-4"));
            }};
        }

        protected DataFeed createDataFeed(Long id, String name) {
            DataFeed df = new DataFeed();
            df.setDataFeedId(id);
            df.setDataSourceId(1L);
            df.setDataFeedName(name);
            df.setDescription("desc");
            df.setEndpoint("kafka://host:port/topic");
            df.setParserId(id);
            df.setTags("a,b,c");
            df.setTimestamp(System.currentTimeMillis());
            return df;
        }
    }

    public class DataSourceTest extends StorableTest {
        {
            storableList = new ArrayList<Storable>() {{
                add(createDataSource(1l, "datasource-1"));
                add(createDataSource(1l, "datasource-2"));
                add(createDataSource(2l, "datasource-3"));
                add(createDataSource(3l, "datasource-4"));
            }};
        }

        protected DataSource createDataSource(Long id, String name) {
            DataSource ds = new DataSource();
            ds.setDataSourceId(id);
            ds.setDataSourceName(name);
            ds.setDescription("desc");
            ds.setTags("t1, t2, t3");
            ds.setTimestamp(System.currentTimeMillis());
            ds.setType(DataSource.Type.DEVICE);
            ds.setTypeConfig("device_type_config");
            return ds;
        }
    }

    public class DeviceTest extends StorableTest {
        {
            storableList = new ArrayList<Storable>() {{
                add(createDevice("device-1", 0l, 1l));
                add(createDevice("device-1", 0l, 2l));
                add(createDevice("device-2", 2l, 2l));
                add(createDevice("device-3", 3l, 3l));
            }};
        }

        protected Device createDevice(String id, Long version, Long datafeedId) {
            Device d = new Device();
            d.setDeviceId(id);
            d.setVersion(version);
            d.setDataSourceId(datafeedId);
            return d;
        }
    }

    /**
     * @return When we add a new implementation for StorageManager interface we will also add a corresponding test implementation
     * which will extends this class and implement this method.
     * <p/>
     * Essentially we are going to run the same test defined in this class for every single implementation of StorageManager.
     */
    protected abstract StorageManager getStorageManager();

    /**
     * Each of the storable entities has its own list and the 0th and 1st index items in that list has same id so
     * test will use that to test the update operation. the 3rd item is inserted at storage layer and 4th i
     */
    @Test
    public void testCrudForAllEntities() {
        for (StorableTest test : storableTests) {
            try {
                test.init();
                test.test();
            } finally {
                test.close();
            }
        }
    }


    /*@Test
    public void testCrudForAllEntities() {
        log.debug("testCrudForAllEntities");
        for (List<Storable> storableList : this.storables) {
            doTestCrudForEntities(storableList);
        }
    }*/

    private void doTestCrudForEntities(List<Storable> storableList) {
        Storable storable1 = storableList.get(0);
        Storable storable2 = storableList.get(1);
        Storable storable3 = storableList.get(2);
        Storable storable4 = storableList.get(3);
        String namespace = storable1.getNameSpace();

        //test add by inserting the first item in list.
        getStorageManager().add(storable1);
        Assert.assertEquals(storable1, getStorageManager().get(storable1.getStorableKey()));

        //test update by calling addOrUpdate on second item which should have the same primary key value as first item.
        getStorageManager().addOrUpdate(storable2);
        Assert.assertEquals(storable2, getStorageManager().get(storable1.getStorableKey()));

        //add 3rd item, only added so list operation will return more then one item.
        getStorageManager().addOrUpdate(storable3);
        Assert.assertEquals(storable3, getStorageManager().get(storable3.getStorableKey()));

        //test remove by adding 4th item and removing it.
        getStorageManager().addOrUpdate(storable4);
        Assert.assertEquals(storable4, getStorageManager().get(storable4.getStorableKey()));
        Storable removed = getStorageManager().remove(storable4.getStorableKey());
        Assert.assertNull(getStorageManager().get(storable4.getStorableKey()));
        Assert.assertEquals(storable4, removed);

        //The final state of storage layer should only have 2nd item (updated version of 1st item) and 3rd Item.
        Set<Storable> storableSet = new HashSet<>();
        storableSet.add(storable2);
        storableSet.add(storable3);
        Assert.assertEquals(storableSet, new HashSet(getStorageManager().list(storable2.getStorableKey().getNameSpace())));

        Collection<Storable> found = getStorageManager().find(namespace, buildQueryParamsForPrimaryKey(storable3));
        Assert.assertEquals(found.size(), 1);
        Assert.assertTrue(found.contains(storable3));
    }



    List<CatalogService.QueryParam> buildQueryParamsForPrimaryKey(Storable storable) {
        final Map<Schema.Field, Object> fieldsToVal = storable.getPrimaryKey().getFieldsToVal();
        final List<CatalogService.QueryParam> queryParams = new ArrayList<>(fieldsToVal.size());

        for (Schema.Field field : fieldsToVal.keySet()) {
            CatalogService.QueryParam qp = new CatalogService.QueryParam(field.getName(), fieldsToVal.get(field).toString());
            queryParams.add(qp);
        }

        return queryParams;
    }

    public static ParserInfo createParserInfo(Long id, String name) {
        ParserInfo pi = new ParserInfo();
        pi.setParserId(id);
        pi.setParserName(name);
        pi.setClassName("com.org.apache.TestParser");
        pi.setJarStoragePath("/tmp/parser.jar");
        pi.setParserSchema(new Schema.SchemaBuilder().fields(new Schema.Field("deviceId", Schema.Type.LONG), new Schema.Field("deviceName", Schema.Type.STRING)).build());
        pi.setVersion(0l);
        pi.setTimestamp(System.currentTimeMillis());
        return pi;
    }

    public static DataFeed createDataFeed(Long id, String name) {
        DataFeed df = new DataFeed();
        df.setDataFeedId(id);
        df.setDataSourceId(1L);
        df.setDataFeedName(name);
        df.setDescription("desc");
        df.setEndpoint("kafka://host:port/topic");
        df.setParserId(id);
        df.setTags("a,b,c");
        df.setTimestamp(System.currentTimeMillis());
        return df;
    }

    public static DataSource createDataSource(Long id, String name) {
        DataSource ds = new DataSource();
        ds.setDataSourceId(id);
        ds.setDataSourceName(name);
        ds.setDescription("desc");
        ds.setTags("t1, t2, t3");
        ds.setTimestamp(System.currentTimeMillis());
        ds.setType(DataSource.Type.DEVICE);
        ds.setTypeConfig("device_type_config");
        return ds;
    }

    public static Device createDevice(String id, Long version, Long datafeedId) {
        Device d = new Device();
        d.setDeviceId(id);
        d.setVersion(version);
        d.setDataSourceId(datafeedId);
        return d;
    }
}
