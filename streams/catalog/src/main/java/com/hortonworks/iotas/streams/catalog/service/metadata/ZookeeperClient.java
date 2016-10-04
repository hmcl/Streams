package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.hortonworks.iotas.streams.catalog.exception.ZookeeperClientException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.List;

public class ZookeeperClient {
    public static final RetryPolicy DEFAULT_RETRY_POLICY = new RetryNTimes(3, 500);

    private String zkConnString;
    private RetryPolicy retryPolicy;
    private CuratorFramework zkCli;

    public interface ZkConnectionStringFactory {
        String createZkConnString();
    }

    public interface ZkPathFactory {
        String createPath();
    }

    private ZookeeperClient(String zkConnString, RetryPolicy retryPolicy, CuratorFramework zkCli) {
        this.zkConnString = zkConnString;
        this.retryPolicy = retryPolicy;
        this.zkCli = zkCli;
    }

    public ZookeeperClient(CuratorFramework zkCli) {
        this.zkCli= zkCli;
        this.zkConnString = zkCli.getZookeeperClient().getCurrentConnectionString();
        this.retryPolicy = zkCli.getZookeeperClient().getRetryPolicy();
    }

    public static ZookeeperClient newInstance(String zkConnString, RetryPolicy retryPolicy) {
        return new ZookeeperClient(zkConnString, retryPolicy, CuratorFrameworkFactory.newClient(zkConnString, retryPolicy));
    }

    public static  ZookeeperClient newInstance(ZkConnectionStringFactory zkConnStrFactory) {
        return newInstance(zkConnStrFactory.createZkConnString(), DEFAULT_RETRY_POLICY);
    }

    public static ZookeeperClient newInstance(ZkConnectionStringFactory zkConnStrFactory, RetryPolicy retryPolicy) {
        return newInstance(zkConnStrFactory.createZkConnString(), retryPolicy);
    }

    public void start() {
        zkCli.start();
    }

    public void close() {
        zkCli.close();
    }

    public List<String> getChildren(String zkPath) throws ZookeeperClientException {
        try {
            return zkCli.getChildren().forPath(zkPath);
        } catch (Exception e) {
            throw new ZookeeperClientException(e);
        }
    }

    public List<String> getChildren(ZkPathFactory zkPathFactory) throws ZookeeperClientException {
        return getChildren(zkPathFactory.createPath());
    }

    public byte[] getData(String zkPath) throws ZookeeperClientException {
        try {
            return zkCli.getData().forPath(zkPath);
        } catch (Exception e) {
            throw new ZookeeperClientException(e);
        }
    }

    public byte[] getData(ZkPathFactory zkPathFactory) throws ZookeeperClientException {
        return getData(zkPathFactory.createPath());
    }

    public CuratorFramework getCuratorFrameworkZkCli() {
        return zkCli;
    }

    public String getZkConnString() {
        return zkConnString;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }
}
