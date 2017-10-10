package org.apache.kafka.connect.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TimerTask;

/**
 * Read object and meta data from
 */

public abstract class  S3Watcher  extends TimerTask {

    private final static Logger log = LoggerFactory.getLogger(S3Watcher.class);
    private String regionName =   null;
    private String serviceEndpoint =  null;
    private Integer maxKeys = 1000;
    private String nextMarker = null;
    private String bucketName = null;


    /** use defaults **/
    public S3Watcher() {
    }

    public S3Watcher(String serviceEndpoint, String regionName) {
        this.regionName = regionName;
        this.serviceEndpoint = serviceEndpoint;
    }

    public S3Watcher(String nextMarker) {
        this.nextMarker = nextMarker;
    }


    public S3Watcher(String serviceEndpoint, String regionName, String bucketName, String nextMarker) {
        this.serviceEndpoint = serviceEndpoint;
        this.regionName = regionName;
        this.bucketName = bucketName;
        this.nextMarker = nextMarker;
    }

    public S3Watcher(String regionName, String bucketName,String nextMarker,  Integer maxKeys) {
        this.regionName = regionName;
        this.bucketName = bucketName;
        this.maxKeys = maxKeys;
        this.nextMarker = nextMarker;
    }

    /**
     * Run the thread.
     */
    public final void run() {
        try {

            log.debug("******************************************");
            log.debug(serviceEndpoint);
            log.debug(regionName);
            log.debug("******************************************");


            AmazonS3 s3 = null ;

            ClientConfiguration cc = new ClientConfiguration();
            if (serviceEndpoint.startsWith("http:")) {
                cc.withSignerOverride("S3SignerType");
                cc.setProtocol(Protocol.HTTP);
            }


            AwsClientBuilder.EndpointConfiguration ec = new AwsClientBuilder.EndpointConfiguration(this.serviceEndpoint, this.regionName);

            s3 = AmazonS3ClientBuilder
                    .standard()
                    .withPathStyleAccessEnabled(true)
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .withEndpointConfiguration(ec)
                    .withClientConfiguration(cc)
                    .build();



//            ClientConfiguration opts = new ClientConfiguration();
//            opts.setSignerOverride("AWSS3V4SignerType");  // NOT "AWS3SignerType"
//            AmazonS3Client s3 = new AmazonS3Client(opts);
//            s3.setEndpoint(serviceEndpoint);

//                    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
//                    .withCredentials(new EnvironmentVariableCredentialsProvider())


            for (Bucket bucket : s3.listBuckets() ) {
                if (bucketName != null && bucket.getName() != bucketName) {
                    continue;
                }
                ObjectListing ol = null;
                do {
                    final ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucket.getName()).withMaxKeys(maxKeys);
                    if (nextMarker != null) {
                        req.withMarker(nextMarker);
                    }
                    ol = s3.listObjects(req);
                    List<S3ObjectSummary> objects = ol.getObjectSummaries();
                    for (S3ObjectSummary os : objects) {
                        S3Object o = s3.getObject(bucket.getName(), os.getKey());
                        ObjectMetadata meta = o.getObjectMetadata();
                        nextMarker = ol.getNextMarker();
                        onObjectFound(bucket, os, meta, nextMarker);
                    }
                } while(ol.isTruncated() == true );
            }
        } catch (AmazonServiceException ase) {
            ase.printStackTrace();
            log.warn("Caught an AmazonServiceException, " +
                    "which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            log.warn("Error Message:    " + ase.getMessage());
            log.warn("HTTP Status Code: " + ase.getStatusCode());
            log.warn("AWS Error Code:   " + ase.getErrorCode());
            log.warn("Error Type:       " + ase.getErrorType());
            log.warn("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            ace.printStackTrace();
            log.warn("Caught an AmazonClientException, " +
                    "which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            log.warn("Error Message: " + ace.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("Continuing", e);
        }
    }

    /**
     * callback, return Object to caller
     * @param bucket
     * @param summary
     * @param meta
     * @param nextMarker
     */
    protected abstract void onObjectFound(Bucket bucket, S3ObjectSummary summary, ObjectMetadata meta, String nextMarker);

}
