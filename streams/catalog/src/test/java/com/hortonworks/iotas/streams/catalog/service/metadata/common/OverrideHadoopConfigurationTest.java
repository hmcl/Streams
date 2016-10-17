package com.hortonworks.iotas.streams.catalog.service.metadata.common;

import com.google.common.collect.Maps;

import com.hortonworks.iotas.streams.catalog.ServiceConfiguration;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import mockit.Expectations;
import mockit.Injectable;
import mockit.MockUp;
import mockit.Mocked;
import mockit.StrictExpectations;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class OverrideHadoopConfigurationTest {
    @Tested
    private OverrideHadoopConfiguration overrideHadoopConfiguration;
    @Mocked
    private StreamCatalogService streamCatalogService;
    @Mocked
    private ServiceConfiguration serviceConfiguration;


    private static class HBaseConfigurationTest extends HBaseConfiguration {

        public HBaseConfigurationTest(Configuration c) {
            super(c);
        }

        @Override
        public synchronized Properties getProps() {
            return super.getProps();
        }

        @Override
        public String toString() {
            return super.toString();
        }
    }

    @Test
    public void override() throws Exception {
        HBaseConfigurationTest mc = new HBaseConfigurationTest(HBaseConfiguration.create());
        final Properties props = mc.getProps();
        final Map<Object, Object> newProps = new HashMap<>(2);

        for (Map.Entry<Object, Object> prop : props.entrySet()) {
            newProps.put(prop.getKey(), prop.getValue());
        }

        OverrideHadoopConfiguration.override(mc, streamCatalogService, ServiceConfigurations.HBASE, 23L, "hbase-site");

        System.out.println(props);
    }

    private static class ConfigurationMock extends MockUp<Configuration> {
        final Properties props = new Properties() {{
            put("p1", "v11");
            put("p2", "v21");
        }};

        synchronized Properties getProps() {
            return props;
        }
    }

    @Test
    public void overrideMockup() throws Exception {
        ConfigurationMock configurationMock = new ConfigurationMock();
        System.out.println(configurationMock.getProps());


        /*OverrideHadoopConfiguration.override(configurationMock, streamCatalogService, ServiceConfigurations.HBASE, 23L, "hbase-site");

        System.out.println(configuration.getProps());*/
    }


    @Test
//    public void overrideMocked(@Injectable final HBaseConfigurationTest configuration) throws Exception {
    public void overrideMocked(@Injectable final HBaseConfigurationTest configuration) throws Exception {
//        final Map<Object, Object> props = new HashMap<Object, Object>() {{put("p1", "v11"); put("p2", "v21");}};
        final Properties props = new Properties() {{
            put("k1", "v11");
            put("k2", "v21");
        }};

        final Map<String, String> newProps = new HashMap<String, String>(){{put("k1", "v12"); put("k2", "v22");}};

        new Expectations() {{
//            configuration.getProps(); result = newProps;
            serviceConfiguration.getConfigurationMap(); result = newProps;
        }};



//        System.out.println(configuration.getProps().toString());
//        Assert.assertEquals(configuration.getProps(), newProps);

        /*new Expectations() {{
            configuration.getProps(); result = props;
        }};*/

        HBaseConfigurationTest config = new HBaseConfigurationTest(HBaseConfiguration.create());
        Properties configProps = config.getProps();
        System.out.println(configProps);

        Assert.assertFalse(configProps.containsKey("k1"));
        Assert.assertFalse(configProps.containsKey("k2"));
        configProps.putAll(props);
        Assert.assertTrue(configProps.containsKey("k1"));
        Assert.assertTrue(configProps.containsKey("k2"));

        Assert.assertTrue(configProps.get("k1").equals("v11"));
        Assert.assertTrue(configProps.get("k2").equals("v21"));

        OverrideHadoopConfiguration.override(config, streamCatalogService, ServiceConfigurations.HBASE, 23L, "hbase-site");

        Assert.assertEquals(configProps.get("k1"), "v12");
        Assert.assertTrue(configProps.get("k2").equals("v22"));
        System.out.println(configProps);
    }
}