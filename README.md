# kafka-connect-directory-source

The connector is used to push file system events (additions and modifications of files in a directory) to Kafka.

Note: this is a hard fork.
* gradle build
* S3DirectorySourceConnector added, works with `aws` and `swift`
* tests added


# Building
*  `gradle shadowJar`

# Sample Configuration - Start with embedded DirWatcher

``` ini
name=directory-source
tasks.max=1 # not really needed
connector.class=org.apache.kafka.connect.directory.DirectorySourceConnector
directories.paths=./tmp1,./tmp2,./tmp3
check.dir.ms=1000 # optional, default is 1000
schema.name=directory_schema
topic=directory_topic
```

or as a JSON

```json
{
	"name": "directory-source",
	"tasks.max": 1,
	"connector.class": "org.apache.kafka.connect.directory.DirectorySourceConnector",
	"schema.name": "directory_schema",
	"topic": "directory_topic",
	"check.dir.ms": 1000,
	"directories.paths": "/path/to/dir/1,/path/to/dir/2,/path/to/dir/3",
}
```

For S3DirectorySourceConnector:

```json
{ "name": "s3-source",
  "config": {
   "tasks.max": 1,
   "connector.class": "org.apache.kafka.connect.directory.S3DirectorySourceConnector",
   "topic": "s3-topic",
   "consumer.max.poll.records": 1000,
   "task.shutdown.graceful.timeout.ms":30000,
   "bucket": "etl-development",
   "interval_ms": "60000",
   "region_name": "us-west-2",
   "bucket_names": "etl-development",
   "schema_name": "s3",
   "service_endpoint": "http://10.96.11.20:8080",
   "key.converter": "org.apache.kafka.connect.storage.StringConverter",
   "value.converter": "org.apache.kafka.connect.storage.StringConverter",
   "internal.key.converter": "org.apache.kafka.connect.json.JsonConverter",
   "internal.value.converter": "org.apache.kafka.connect.json.JsonConverter",
   "internal.key.converter.schemas.enable": false,
   "internal.value.converter.schemas.enable": false
  }
 }
```





- **name**: name of the connector
- **connector.class**: class of the implementation of the connector
- **schema.name**: name to use for the schema
- **topic**: name of the topic to append to
- **directories.paths**: comma separated list of directories to watch, one per task
- **check.dir.ms**: interval at which to check for updates in the directories