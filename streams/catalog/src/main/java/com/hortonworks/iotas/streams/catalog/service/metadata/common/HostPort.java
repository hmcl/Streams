package com.hortonworks.iotas.streams.catalog.service.metadata.common;

public class HostPort {
    private final String host;
    private final int port;

    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
