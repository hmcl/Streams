#  CREATE DATABASE IF NOT EXISTS iotas;

DROP TABLE IF EXISTS devices;

CREATE TABLE IF NOT EXISTS devices (
    deviceId VARCHAR(64) NOT NULL,
    version BIGINT NOT NULL,
    dataSourceId BIGINT NOT NULL,
    PRIMARY KEY (deviceId, version)
);