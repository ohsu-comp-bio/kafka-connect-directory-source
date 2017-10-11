package org.apache.kafka.connect.directory;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.utils.AbstractS3Test;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by walsbr on 10/8/17.
 */
public class S3DirectorySourceTaskTest extends AbstractS3Test {

    private final static Logger log = LoggerFactory.getLogger(S3DirectorySourceTaskTest.class);

    @Test
    public void testNormalLifecycle() throws InterruptedException {
        List<String> objects = new ArrayList<>();
        S3DirectorySourceTask task = new S3DirectorySourceTask();
        Map<String,String> props = new HashMap<>();
        props.put(S3DirectorySourceConnector.SERVICE_ENDPOINT, "http://localhost:8001");
        props.put(S3DirectorySourceConnector.REGION_NAME, "us-west-2");
        props.put(S3DirectorySourceConnector.SCHEMA_NAME, "s3");
        props.put(S3DirectorySourceConnector.TOPIC, "dummy");
        props.put(S3DirectorySourceConnector.INTERVAL_MS, "60000");
        props.put(S3DirectorySourceConnector.BUCKET_NAMES, "testbucket");
        task.start(props);
        List<SourceRecord> records = new ArrayList<>();
        while(records.size() < 2) {
            log.warn("polling()");
            records.addAll(task.poll());
        }
        task.stop();
        assert records.size() == 2;
    }
}
