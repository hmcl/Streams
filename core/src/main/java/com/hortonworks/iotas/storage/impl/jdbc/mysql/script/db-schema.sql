#  CREATE DATABASE IF NOT EXISTS iotas;

DROP TABLE IF EXISTS devices;

CREATE TABLE IF NOT EXISTS devices (
    deviceId VARCHAR(64) NOT NULL,
    version BIGINT NOT NULL,
    dataSourceId BIGINT NOT NULL,
    PRIMARY KEY (deviceId, version)
);

SELECT * FROM devices;

INSERT INTO devices (deviceId, version, dataSourceId) VALUES ('id', 123, 456)
    ON DUPLICATE KEY UPDATE deviceId='id', version=123, dataSourceId=456;
