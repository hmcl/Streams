 CREATE DATABASE IF NOT EXISTS iotas;
 USE iotas;

#  Device
CREATE TABLE IF NOT EXISTS devices (
    deviceId VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL,
    dataSourceId BIGINT NOT NULL,
    PRIMARY KEY (deviceId, version), 
    FOREIGN KEY (dataSourceId) REFERENCES datasources(dataSourceId)
);

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
    schemaId INT NOT NULL,
    timestamp  BIGINT,
    PRIMARY KEY (parserId),
    UNIQUE (parserName),
    FOREIGN KEY (schemaId) REFERENCES `schema`(id)
);

CREATE TABLE IF NOT EXISTS `schema` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS schema_field (
    name VARCHAR(64) NOT NULL PRIMARY KEY,
    type ENUM('BOOLEAN', 'BYTE', 'SHORT', 'INTEGER', 'LONG', 'FLOAT', 'DOUBLE', 'STRING', 'BINARY', 'NESTED', 'ARRAY'), # TODO: Create table schema_field_type for type?
    schema_id INT,
    PRIMARY KEY(name, type),
    FOREIGN KEY (schema_id) REFERENCES `schema`(id)
);


