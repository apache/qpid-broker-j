/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.logging;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;


/**
 * Test: UnitTestMessageLoggerTest
 * <p>
 * This test verifies that UnitTestMessageLogger adheres to its interface.
 * <p>
 * Messages are logged, and Throwables recorded in an array that can be
 * retrieved and cleared.
 */
public class UnitTestMessageLoggerTest extends UnitTestBase
{
    private static final String TEST_MESSAGE = "Test";
    private static final String TEST_THROWABLE = "Test Throwable";
    private static final String TEST_HIERARCHY = "test.hierarchy";

    @Test
    public void testRawMessage()
    {
        UnitTestMessageLogger logger = new UnitTestMessageLogger();

        assertEquals("Messages logged before test start", (long) 0, (long) logger.getLogMessages().size());

        // Log a message
        logger.rawMessage(TEST_MESSAGE, TEST_HIERARCHY);

        List<Object> messages = logger.getLogMessages();

        assertEquals("Expected to have 1 messages logged", (long) 1, (long) messages.size());

        assertEquals("First message not what was logged", TEST_MESSAGE, messages.get(0));
    }

    @Test
    public void testRawMessageWithThrowable()
    {
        UnitTestMessageLogger logger = new UnitTestMessageLogger();

        assertEquals("Messages logged before test start", (long) 0, (long) logger.getLogMessages().size());

        // Log a message
        Throwable throwable = new Throwable(TEST_THROWABLE);

        logger.rawMessage(TEST_MESSAGE, throwable, TEST_HIERARCHY);

        List<Object> messages = logger.getLogMessages();

        assertEquals("Expected to have 2 entries", (long) 2, (long) messages.size());

        assertEquals("Message text not what was logged", TEST_MESSAGE, messages.get(0));

        assertEquals("Message throwable not what was logged",
                            TEST_THROWABLE,
                            ((Throwable) messages.get(1)).getMessage());

    }

    @Test
    public void testClear()
    {
        UnitTestMessageLogger logger = new UnitTestMessageLogger();

        assertEquals("Messages logged before test start", (long) 0, (long) logger.getLogMessages().size());

        // Log a message
        logger.rawMessage(TEST_MESSAGE, null, TEST_HIERARCHY);

        assertEquals("Expected to have 1 messages logged", (long) 1, (long) logger.getLogMessages().size());

        logger.clearLogMessages();

        assertEquals("Expected to have no messages after a clear",
                            (long) 0,
                            (long) logger.getLogMessages().size());
    }
}
