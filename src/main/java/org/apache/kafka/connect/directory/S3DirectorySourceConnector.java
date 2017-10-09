package org.apache.kafka.connect.directory;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * DirectorySourceConnector implements the connector interface
 * to write on Kafka file system events (creations, modifications etc)
 *
 * @author Sergio Spinatelli
 */
public class S3DirectorySourceConnector extends SourceConnector {
    public static final String INTERVAL_MS = "interval_ms";
    public static final String SCHEMA_NAME = "schema_name";
    public static final String TOPIC = "topic";
    public static final String BUCKET_NAMES = "bucket_names";
    public static final String SERVICE_ENDPOINT = "service_endpoint";
    public static final String REGION_NAME = "region_name";

    // common properties
    private String schema_name;
    private String topic;

    // our properties
    private String bucket_names;
    private String interval_ms;
    private String service_endpoint;
    private String region_name;


    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SERVICE_ENDPOINT, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "S3 compatible endpoint (aws/swift).")
            .define(REGION_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "S3 Region name.")
            .define(BUCKET_NAMES, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "Comma separated list of buckets to watch, one per task. Defaults to all buckets. If specified, count of buckets must agree with maxTasks")
            .define(INTERVAL_MS, ConfigDef.Type.INT, ConfigDef.Importance.LOW, "Interval at which to check for updates in the buckets. Defaults to 60 secs.")
            ;



    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }


    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        schema_name = props.get(SCHEMA_NAME);
        if (schema_name == null || schema_name.isEmpty())
            throw new ConnectException("missing schema.name");

        topic = props.get(TOPIC);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("missing topic");


        interval_ms = props.get(INTERVAL_MS);
        if (interval_ms == null || interval_ms.isEmpty())
            interval_ms = "60000";

        bucket_names = props.get(BUCKET_NAMES);

        region_name = props.get(REGION_NAME);
        if (region_name == null || region_name.isEmpty())
            region_name = "us-west-2";


        service_endpoint = props.get(SERVICE_ENDPOINT);
        if (service_endpoint == null || service_endpoint.isEmpty())
            service_endpoint = "https://s3.us-west-2.amazonaws.com" ;

    }


    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return S3DirectorySourceTask.class;
    }


    /**
     * Returns a set of configurations for the Tasks based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        if (bucket_names == null) {
            Map<String, String> config = new HashMap<>();
            config.put(INTERVAL_MS, interval_ms);
            config.put(SCHEMA_NAME, schema_name);
            config.put(TOPIC, topic);
            config.put(REGION_NAME,region_name);
            config.put(SERVICE_ENDPOINT,service_endpoint);
            configs.add(config);
        } else {
            List<String> buckets = Arrays.asList(bucket_names.split(","));
            for (int i = 0; i < buckets.size(); i++) {
                Map<String, String> config = new HashMap<>();
                config.put(INTERVAL_MS, interval_ms);
                config.put(SCHEMA_NAME, schema_name);
                config.put(TOPIC, topic);
                config.put(S3DirectorySourceTask.BUCKET, buckets.get(i));
                config.put(REGION_NAME,region_name);
                config.put(SERVICE_ENDPOINT,service_endpoint);
                configs.add(config);
            }
        }
        return configs;
    }


    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

}
