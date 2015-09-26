package com.hortonworks.iotas.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;

import java.util.HashMap;
import java.util.Map;


/**
 *
 */
public class DataFeed implements Storable {
    public static final String NAME_SPACE = "datafeeds";
    public static final String DATAFEED_ID = "dataFeedId";
    public static final String DATASOURCE_ID = "dataSourceId";
    public static final String DATAFEED_NAME = "dataFeedName";
    public static final String DESCRIPTION = "description";
    public static final String TAGS = "tags";
    public static final String PARSER_ID = "parserId";
    public static final String ENDPOINT = "endpoint";
    public static final String TIME_STAMP = "timestamp";

    /**
     * Unique Id, this is the primary key.
     */
    private Long dataFeedId;

    /**
     * The foreign key reference to data source.
     */
    private Long dataSourceId;

    /**
     * Human readable name.
     */
    private String dataFeedName;

    /**
     * Human readable description.
     */
    private String description;

    /**
     * Free form tag strings like "Social, Device, Weather"
     */
    private String tags;

    /**
     * Foreign key reference to a parser info that defines which parser implementation can be used to parse this feed.
     */
    private Long parserId;

    /**
     * Where is the actual data for this feed being pushed. i.e "kafka:\\host1:port\nest-device-data-topic", "twitter:\\twitter-api.host:port\feedname"
     */
    private String endpoint;

    /**
     * Time this feed was created/updated.

     */
    private Long timestamp;

    @JsonIgnore
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @JsonIgnore
    public Schema getSchema() {
        return new Schema.SchemaBuilder().fields(
                new Schema.Field(DATAFEED_ID, Schema.Type.LONG),
                new Schema.Field(DATASOURCE_ID, Schema.Type.LONG),
                new Schema.Field(DATAFEED_NAME, Schema.Type.STRING),
                new Schema.Field(DESCRIPTION, Schema.Type.STRING),
                new Schema.Field(TAGS, Schema.Type.STRING),
                new Schema.Field(PARSER_ID, Schema.Type.LONG),
                new Schema.Field(ENDPOINT, Schema.Type.STRING),
                new Schema.Field(TIME_STAMP, Schema.Type.LONG)
        ).build();
    }

    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> fieldToObjectMap = new HashMap<Schema.Field, Object>();
        fieldToObjectMap.put(new Schema.Field(DATAFEED_ID, Schema.Type.LONG), this.dataFeedId);
        return new PrimaryKey(fieldToObjectMap);
    }

    @JsonIgnore
    public StorableKey getStorableKey() {
        return new StorableKey(getNameSpace(), getPrimaryKey());
    }

    public Map toMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(DATAFEED_ID, this.dataFeedId);
        map.put(DATASOURCE_ID, this.dataSourceId);
        map.put(DATAFEED_NAME, this.dataFeedName);
        map.put(DESCRIPTION, this.description);
        map.put(TAGS, this.tags);
        map.put(PARSER_ID, this.parserId);
        map.put(ENDPOINT, this.endpoint);
        map.put(TIME_STAMP, this.timestamp);
        return map;
    }

    public Storable fromMap(Map<String, Object> map) {
        this.dataFeedId = (Long) map.get(DATAFEED_ID);
        this.dataSourceId = (Long) map.get(DATASOURCE_ID);
        this.dataFeedName = (String)  map.get(DATAFEED_NAME);
        this.description = (String)  map.get(DESCRIPTION);
        this.tags = (String)  map.get(TAGS);
        this.parserId = (Long) map.get(PARSER_ID);
        this.endpoint = (String) map.get(ENDPOINT);
        this.timestamp = (Long) map.get(TIME_STAMP);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataFeed)) return false;

        DataFeed dataFeed = (DataFeed) o;

        if (!dataFeedId.equals(dataFeed.dataFeedId)) return false;
        if (!dataSourceId.equals(dataFeed.dataSourceId)) return false;
        if (!dataFeedName.equals(dataFeed.dataFeedName)) return false;
        if (description != null ? !description.equals(dataFeed.description) : dataFeed.description != null)
            return false;
        if (tags != null ? !tags.equals(dataFeed.tags) : dataFeed.tags != null) return false;
        if (!parserId.equals(dataFeed.parserId)) return false;
        if (!endpoint.equals(dataFeed.endpoint)) return false;
        return timestamp.equals(dataFeed.timestamp);

    }

    @Override
    public int hashCode() {
        int result = dataFeedId.hashCode();
        result = 31 * result + dataSourceId.hashCode();
        result = 31 * result + dataFeedName.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        result = 31 * result + parserId.hashCode();
        result = 31 * result + endpoint.hashCode();
        result = 31 * result + timestamp.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DataFeed{" +
                "dataFeedId=" + dataFeedId +
                ", dataSourceId=" + dataSourceId +
                ", dataFeedName='" + dataFeedName + '\'' +
                ", description='" + description + '\'' +
                ", tags='" + tags + '\'' +
                ", parserId=" + parserId +
                ", endpoint='" + endpoint + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public Long getDataFeedId() {
        return dataFeedId;
    }

    public void setDataFeedId(Long dataFeedId) {
        this.dataFeedId = dataFeedId;
    }

    public Long getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(Long dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public String getDataFeedName() {
        return dataFeedName;
    }

    public void setDataFeedName(String dataFeedName) {
        this.dataFeedName = dataFeedName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public Long getParserId() {
        return parserId;
    }

    public void setParserId(Long parserId) {
        this.parserId = parserId;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
