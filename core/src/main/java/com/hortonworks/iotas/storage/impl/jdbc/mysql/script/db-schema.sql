 CREATE DATABASE IF NOT EXISTS iotas;
 USE iotas;

 CREATE TABLE IF NOT EXISTS datasources (
     dataSourceId BIGINT AUTO_INCREMENT NOT NULL,
     dataSourceName VARCHAR(128) NOT NULL,
     description TEXT,
     tags TEXT,
     type ENUM('DEVICE', 'UNKNOWN') NOT NULL,    # TODO: Create table datasources_type for type?
     typeConfig TEXT,                            # TODO: NOT NULL, ???
     timestamp  BIGINT,
     PRIMARY KEY (dataSourceId)
 );

CREATE TABLE IF NOT EXISTS devices (
    deviceId VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL,
    dataSourceId BIGINT NOT NULL,
    PRIMARY KEY (deviceId, version), 
    FOREIGN KEY (dataSourceId) REFERENCES datasources(dataSourceId)
);

CREATE TABLE IF NOT EXISTS datafeeds (
    dataFeedId BIGINT AUTO_INCREMENT NOT NULL,
    dataSourceId BIGINT NOT NULL,
    dataFeedName VARCHAR(128) NOT NULL,
    description TEXT,
    tags TEXT,
    parserId BIGINT NOT NULL,
    endpoint TEXT NOT NULL,
    timestamp  BIGINT,
    PRIMARY KEY (dataFeedId),
    FOREIGN KEY (dataSourceId) REFERENCES datasources(dataSourceId)
);

CREATE TABLE IF NOT EXISTS parser_info (
    parserId BIGINT AUTO_INCREMENT NOT NULL,
    parserName VARCHAR(128) NOT NULL,
    version BIGINT,                             # TODO: NOT NULL ???
    className TEXT NOT NULL,
    jarStoragePath TEXT NOT NULL,
    `schema` TEXT NOT NULL,
    timestamp  BIGINT,
    PRIMARY KEY (parserId),
    UNIQUE (parserName)
);
