package org.apache.kafka.connect.utils;

import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.io.IOException;


/**
 * apparently does nothing ...! (bw)
 * @author Sergio Spinatelli
 */
public class DirWatcherTest {

    @Before
    public void setup() throws IOException {
    }

    @Test
    public void testNormalLifecycle() {
    }

    private void replay() {
        PowerMock.replayAll();
    }

}