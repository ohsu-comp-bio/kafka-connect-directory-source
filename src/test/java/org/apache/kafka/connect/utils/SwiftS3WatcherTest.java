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
 * Created by walsbr on 10/9/17.
 */
public class SwiftS3WatcherTest  {



    final static  org.slf4j.Logger logger = LoggerFactory.getLogger(SwiftS3WatcherTest.class);


//    // UNCOMMENT TO ENABLE HTTP LEVEL DEBUGGING
//    static {
//        System.setProperty("org.apache.commons.logging.Log","org.apache.commons.logging.impl.SimpleLog");
//        System.setProperty("org.apache.commons.logging.simplelog.showdatetime","true");
//        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http","DEBUG");
//        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire","DEBUG");
//    }


    @Before
    public void setup() throws IOException {
//        System.getProperties().put("aws.accessKeyId", "724596f1fe184f02b8c682fa2734a4e6");
//        System.getProperties().put("aws.secretKey", "ae40a4e76d1d4aeea74b85142b18a8fb");
        logger.info("error {}", logger.isErrorEnabled());
        logger.info("info {}", logger.isInfoEnabled());
        logger.info("debug {}", logger.isDebugEnabled());

    }


    @Test
    public void testNormalLifecycle() throws InterruptedException {
        List<String> objects = new ArrayList<>();
        S3Watcher task = new S3Watcher("http://10.96.11.20:8080", "RegionOne") {
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
                    e.printStackTrace();
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date());
        Thread.sleep(10000);

        assert objects.size() > 2;

    }

}
