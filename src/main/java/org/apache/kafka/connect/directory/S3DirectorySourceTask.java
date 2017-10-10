package org.apache.kafka.connect.directory;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.utils.S3Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * DirectorySourceTask is a Task that reads changes from a directory for storage
 * new binary detected files in Kafka.
 *
 * @author Alex Piermatteo
 */
public class S3DirectorySourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(S3DirectorySourceTask.class);
    public static final String MARKER = "marker";
    public static final String BUCKET = "bucket";

    private TimerTask task;
    private String topic;
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private ConcurrentLinkedQueue<SourceRecord> recordQueue = new ConcurrentLinkedQueue<>();


    @Override
    public String version() {
        return new S3DirectorySourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {

        log.warn("********** S3DirectorySourceConnector PROPERTIES START");
        Map<String, String> env = System.getenv();
        for (String envName : env.keySet()) {
            log.warn(envName + "=" + env.get(envName));
        }
        log.warn("********** S3DirectorySourceConnector PROPERTIES END");

        String schemaName = props.get(S3DirectorySourceConnector.SCHEMA_NAME);

        topic = props.get(S3DirectorySourceConnector.TOPIC);

        String interval_ms = props.get(S3DirectorySourceConnector.INTERVAL_MS);

        String marker = props.get(MARKER);

        String bucketName = props.get(BUCKET);

        // override marker with partition, if it exists
        if (bucketName != null) {
            Map<String, Object> off = context.offsetStorageReader().offset(offsetKey(bucketName));
            if (off != null)
                marker = (String) off.get(MARKER);
        }

        String region_name = props.get(S3DirectorySourceConnector.REGION_NAME);
        String service_endpoint = props.get(S3DirectorySourceConnector.SERVICE_ENDPOINT);

        task = new S3Watcher(service_endpoint,region_name, bucketName, marker ) {
            @Override
            protected void onObjectFound(Bucket bucket, S3ObjectSummary summary, ObjectMetadata meta, String nextMarker) {
                try {
                    log.warn(bucket.getName());
                    log.warn("     k:" + summary.getKey());
                    log.debug("  etag:" + summary.getETag());
                    log.debug("  meta:" + meta);
                    log.debug("  mark:" + nextMarker);
                    ObjectMapper objectMapper = new ObjectMapper();
                    Map<String,Object> objectMap = new HashMap<>();
                    objectMap.put("summary", summary);
                    objectMap.put("meta",meta);
                    recordQueue.add(new SourceRecord(offsetKey(bucket.getName()), offsetValue(nextMarker), topic, VALUE_SCHEMA, objectMapper.writeValueAsString(objectMap)));
                } catch (JsonProcessingException e) {
                    log.warn("onObjectFound",e);
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date(), Long.parseLong(interval_ms));
    }


    /**
     * Poll this DirectorySourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptException {
        List<SourceRecord> records = new ArrayList<>();
        //consume the queue
        while (!recordQueue.isEmpty()) {
            records.add(recordQueue.poll());
        }
        log.warn("poll() returning " + records.size() + " elements");
        return records;
    }


    private Map<String, String> offsetKey(String name) {
        return Collections.singletonMap(BUCKET, name);
    }

    private Map<String, Object> offsetValue(String name) {
        return Collections.singletonMap(MARKER, name);
    }



    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        task.cancel();
    }

}
