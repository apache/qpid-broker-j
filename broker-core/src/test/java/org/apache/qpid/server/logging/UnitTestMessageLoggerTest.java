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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

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
        final UnitTestMessageLogger logger = new UnitTestMessageLogger();

        assertEquals(0, (long) logger.getLogMessages().size(), "Messages logged before test start");

        // Log a message
        logger.rawMessage(TEST_MESSAGE, TEST_HIERARCHY);

        final List<Object> messages = logger.getLogMessages();

        assertEquals(1, (long) messages.size(), "Expected to have 1 messages logged");

        assertEquals(TEST_MESSAGE, messages.get(0), "First message not what was logged");
    }

    @Test
    public void testRawMessageWithThrowable()
    {
        final UnitTestMessageLogger logger = new UnitTestMessageLogger();

        assertEquals(0, (long) logger.getLogMessages().size(), "Messages logged before test start");

        // Log a message
        final Throwable throwable = new Throwable(TEST_THROWABLE);

        logger.rawMessage(TEST_MESSAGE, throwable, TEST_HIERARCHY);

        final List<Object> messages = logger.getLogMessages();

        assertEquals(2, (long) messages.size(), "Expected to have 2 entries");

        assertEquals(TEST_MESSAGE, messages.get(0), "Message text not what was logged");

        assertEquals(TEST_THROWABLE, ((Throwable) messages.get(1)).getMessage(),
                "Message throwable not what was logged");
    }

    @Test
    public void testClear()
    {
        final UnitTestMessageLogger logger = new UnitTestMessageLogger();

        assertEquals(0, (long) logger.getLogMessages().size(), "Messages logged before test start");

        // Log a message
        logger.rawMessage(TEST_MESSAGE, null, TEST_HIERARCHY);

        assertEquals(1, (long) logger.getLogMessages().size(), "Expected to have 1 messages logged");

        logger.clearLogMessages();

        assertEquals(0, (long) logger.getLogMessages().size(), "Expected to have no messages after a clear");
    }
}
