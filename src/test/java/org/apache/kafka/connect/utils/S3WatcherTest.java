package org.apache.kafka.connect.utils;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;


/**
 * test against local embedded s3 source
 */
public class S3WatcherTest extends AbstractS3Test {

    private final static Logger log = LoggerFactory.getLogger(S3WatcherTest.class);

    @Test
    public void testNormalLifecycle() throws InterruptedException {
        List<String> objects = new ArrayList<>();
        S3Watcher task = new S3Watcher("http://localhost:8001", "us-west-2") {
            @Override
            protected void onObjectFound(Bucket bucket, S3ObjectSummary summary, ObjectMetadata meta, String nextMarker) {
                try {
                    System.out.println(bucket.getName());
                    System.out.println("     k:" + summary.getKey());
                    System.out.println("  etag:" + summary.getETag());
                    System.out.println("  meta:" + meta);
                    System.out.println("  mark:" + nextMarker);
                    ObjectMapper objectMapper = new ObjectMapper();
                    Map<String,Object> objectMap = new HashMap<>();
                    objectMap.put("summary", summary);
                    objectMap.put("meta", meta);
                    System.out.println("  json:" + objectMapper.writeValueAsString(objectMap));
                    objects.add(objectMapper.writeValueAsString(objectMap));
                } catch (Exception e) {
                    log.warn("onObjectFound", e);
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date());
        Thread.sleep(1000);

        assert objects.size() == 2;

    }

}