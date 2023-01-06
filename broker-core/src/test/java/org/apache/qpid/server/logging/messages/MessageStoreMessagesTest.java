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
package org.apache.qpid.server.logging.messages;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Test MST Log Messages
 */
public class MessageStoreMessagesTest extends AbstractTestMessages
{
    @Test
    public void testMessageStoreCreated()
    {
        _logMessage = MessageStoreMessages.CREATED();
        final List<Object> log = performLog();

        final String[] expected = {"Created"};

        validateLogMessage(log, "MST-1001", expected);
    }

    @Test
    public void testMessageStoreStoreLocation()
    {
        final String location = "/path/to/the/message/store.files";

        _logMessage = MessageStoreMessages.STORE_LOCATION(location);
        final List<Object> log = performLog();

        final String[] expected = {"Store location :", location};

        validateLogMessage(log, "MST-1002", expected);
    }

    @Test
    public void testMessageStoreClosed()
    {
        _logMessage = MessageStoreMessages.CLOSED();
        final List<Object> log = performLog();

        final String[] expected = {"Closed"};

        validateLogMessage(log, "MST-1003", expected);
    }

    @Test
    public void testMessageStoreRecoveryStart()
    {
        _logMessage = MessageStoreMessages.RECOVERY_START();
        final List<Object> log = performLog();

        final String[] expected = {"Recovery Start"};

        validateLogMessage(log, "MST-1004", expected);
    }
}
