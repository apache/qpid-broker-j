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

import org.junit.Test;

/**
 * Test QUE Log Messages
 */
public class QueueMessagesTest extends AbstractTestMessages
{
    @Test
    public void testQueueCreatedALL()
    {
        String owner = "guest";
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid",owner, priority, true, true, true, true, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "Owner:", owner, "AutoDelete",
                             "Durable", "Transient", "Priority:",
                             String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedOwnerAutoDelete()
    {
        String owner = "guest";

        _logMessage = QueueMessages.CREATED("uuid", owner, null, true, true, false, false, false);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "Owner:", owner, "AutoDelete"};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedOwnerPriority()
    {
        String owner = "guest";
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid", owner, priority, true, false, false, false, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "Owner:", owner, "Priority:",
                             String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedOwnerAutoDeletePriority()
    {
        String owner = "guest";
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid", owner, priority, true, true, false, false, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "Owner:", owner, "AutoDelete",
                             "Priority:",
                             String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedOwnerAutoDeleteTransient()
    {
        String owner = "guest";

        _logMessage = QueueMessages.CREATED("uuid", owner, null, true, true, false, true, false);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "Owner:", owner, "AutoDelete",
                             "Transient"};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedOwnerAutoDeleteTransientPriority()
    {
        String owner = "guest";
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid", owner, priority, true, true, false, true, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "Owner:", owner, "AutoDelete",
                             "Transient", "Priority:",
                             String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedOwnerAutoDeleteDurable()
    {
        String owner = "guest";

        _logMessage = QueueMessages.CREATED("uuid", owner, null, true, true, true, false, false);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "Owner:", owner, "AutoDelete",
                             "Durable"};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedOwnerAutoDeleteDurablePriority()
    {
        String owner = "guest";
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid", owner, priority, true, true, true, false, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "Owner:", owner, "AutoDelete",
                             "Durable", "Priority:",
                             String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedAutoDelete()
    {
        _logMessage = QueueMessages.CREATED("uuid", null, null, false, true, false, false, false);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid", "AutoDelete"};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedPriority()
    {
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid", null, priority, false, false, false, false, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid",
                             "Priority:", String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedAutoDeletePriority()
    {
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid", null, priority, false, true, false, false, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "AutoDelete",
                             "Priority:",
                             String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedAutoDeleteTransient()
    {
        _logMessage = QueueMessages.CREATED("uuid", null, null, false, true, false, true, false);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid",
                             "AutoDelete", "Transient"};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedAutoDeleteTransientPriority()
    {
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid", null, priority, false, true, false, true, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid",
                             "AutoDelete", "Transient", "Priority:",
                String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedAutoDeleteDurable()
    {
        _logMessage = QueueMessages.CREATED("uuid", null, null, false, true, true, false, false);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid",
                             "AutoDelete", "Durable"};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueCreatedAutoDeleteDurablePriority()
    {
        Integer priority = 3;

        _logMessage = QueueMessages.CREATED("uuid", null, priority, false, true, true, false, true);
        List<Object> log = performLog();

        String[] expected = {"Create :", "ID:", "uuid",
                             "AutoDelete", "Durable", "Priority:",
                String.valueOf(priority)};

        validateLogMessage(log, "QUE-1001", expected);
    }

    @Test
    public void testQueueDeleted()
    {
        _logMessage = QueueMessages.DELETED("uuid");
        List<Object> log = performLog();

        String[] expected = {"Deleted", "ID:", "uuid"};

        validateLogMessage(log, "QUE-1002", expected);
    }

}
