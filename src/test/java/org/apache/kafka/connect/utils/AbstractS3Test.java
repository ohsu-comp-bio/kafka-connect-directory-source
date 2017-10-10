package org.apache.kafka.connect.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import io.findify.s3mock.S3Mock;


/**
 * bring up local embedded s3 source
 */
public class AbstractS3Test {

    S3Mock s3Mock = null ;
    private final static Logger log = LoggerFactory.getLogger(AbstractS3Test.class);


    @Before
    public void setup() throws IOException {
    /*
     S3Mock.create(8001, "/tmp/s3");
     */
        s3Mock = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        s3Mock.start();

        /* AWS S3 client setup.
         *  withPathStyleAccessEnabled(true) trick is required to overcome S3 default
         *  DNS-based bucket access scheme
         *  resulting in attempts to connect to addresses like "bucketname.localhost"
         *  which requires specific DNS setup.
         */
        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration("http://localhost:8001", "us-west-2");
        AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();
        client.createBucket("testbucket");
        client.putObject("testbucket", "file/name", "contents");
        byte[] contentBytes = "contents".getBytes(StandardCharsets.UTF_8.name());
        InputStream stream = new ByteArrayInputStream(contentBytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(Long.valueOf(contentBytes.length));
        meta.setContentType("text");
        Map<String,String> userMeta = new HashMap<>();
        userMeta.put("key", "value");
        meta.setUserMetadata(userMeta);
        client.putObject("testbucket", "file/name2", stream, meta);
    }


    @After
    public void tearDown() {
        s3Mock.stop();
    }

}