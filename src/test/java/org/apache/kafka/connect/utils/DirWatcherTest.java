package org.apache.kafka.connect.utils;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.Map;


/**
 * apparently does nothing ...! (bw)
 * @author Sergio Spinatelli
 */
public class DirWatcherTest {



    @Test
    public void testNormalLifecycle() {
        OffsetStorageReader offsetStorageReader = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> map) {
                return null;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> collection) {
                return null;
            }
        };
        DirWatcher dirWatcher = new DirWatcher(offsetStorageReader,"src",null) {
            @Override
            protected void onChange(File file, String action) {
                //System.err.println(file);
            }
        };
        dirWatcher.run();
        assert dirWatcher.getFilesQueue().size() > 0;
    }

}