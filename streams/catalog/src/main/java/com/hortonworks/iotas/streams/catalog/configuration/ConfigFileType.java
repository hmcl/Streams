package com.hortonworks.iotas.streams.catalog.configuration;

import com.google.common.io.Files;

/**
 * Defines configuration file's type. It is needed for writing configuration map to file.
 * @see ConfigFileWriter
 */
public enum ConfigFileType {
  HADOOP_XML, PROPERTIES, YAML;

  public static ConfigFileType getFileTypeFromFileName(String fileName) {
    String fileExt = Files.getFileExtension(fileName);
    if (fileExt.isEmpty()) {
      return null;
    }

    switch (fileExt) {
    case "properties":
      return PROPERTIES;
    case "xml":
      // FIXME: do we want to treat another xml type for the service configuration?
      // If then it shouldn't guess it only based on extension.
      return HADOOP_XML;
    case "yaml":
      return YAML;
    }

    return null;
  }
}
