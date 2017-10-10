package org.apache.kafka.connect.utils;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;


/**
 * test a real AWS bucket
 * Created by walsbr on 10/9/17.
 */
public class AWSS3WatcherTest {

    final static  org.slf4j.Logger log = LoggerFactory.getLogger(AWSS3WatcherTest.class);

//    static {
//        System.setProperty("org.apache.commons.logging.Log","org.apache.commons.logging.impl.SimpleLog");
//        System.setProperty("org.apache.commons.logging.simplelog.showdatetime","true");
//        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http","DEBUG");
//        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire","DEBUG");
//    }


    @Before
    public void setup() throws IOException {
        log.info("error {}", log.isErrorEnabled());
        log.info("info {}", log.isInfoEnabled());
        log.info("debug {}", log.isDebugEnabled());
    }


    @Test
    public void testNormalLifecycle() throws InterruptedException {
        List<String> objects = new ArrayList<>();
        S3Watcher task = new S3Watcher("https://s3-us-west-2.amazonaws.com", "us-west-2") {
            @Override
            protected void onObjectFound(Bucket bucket, S3ObjectSummary summary, ObjectMetadata meta, String nextMarker) {
                try {
                    log.debug(bucket.getName());
                    log.debug("     k:" + summary.getKey());
                    log.debug("  etag:" + summary.getETag());
                    log.debug("   md5:" + meta.getContentMD5());
                    log.debug("  mark:" + nextMarker);
                    ObjectMapper objectMapper = new ObjectMapper();
                    Map<String,Object> objectMap = new HashMap<>();
                    objectMap.put("summary", summary);
                    log.debug("meta", meta);
                    log.debug("  json:" + objectMapper.writeValueAsString(objectMap));
                    objects.add(objectMapper.writeValueAsString(objectMap));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date());
        Thread.sleep(5000);

        assert objects.size() > 2;

    }

}
