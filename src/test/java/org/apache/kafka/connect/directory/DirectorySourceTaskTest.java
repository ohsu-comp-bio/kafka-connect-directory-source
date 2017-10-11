package org.apache.kafka.connect.directory;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by walsbr on 10/8/17.
 */
public class DirectorySourceTaskTest  {

    private final static Logger log = LoggerFactory.getLogger(DirectorySourceTaskTest.class);

    @Test
    public void testNormalLifecycle() throws InterruptedException {
        DirectorySourceTask task = new DirectorySourceTask();
        SourceTaskContext context = new SourceTaskContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> map) {
                        return null;
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> collection) {
                        return null;
                    }
                };

            }
        };
        task.initialize(context);
        Map<String,String> props = new HashMap<>();
        props.put(DirectorySourceConnector.TOPIC, "dummy");
        props.put(DirectorySourceConnector.DIR_PATH, "src");
        props.put(DirectorySourceConnector.SCHEMA_NAME, "dir");
        props.put(DirectorySourceConnector.CHCK_DIR_MS, "60000");

        task.start(props);
        List<SourceRecord> records = new ArrayList<>();
        while(records.size() < 2) {
            log.warn("polling()");
            records.addAll(task.poll());
        }
        task.stop();
        assert records.size() > 0;
    }
}
