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

import org.apache.qpid.server.logging.Outcome;

/**
 * Test VHT Log Messages
 */
public class VirtualHostMessagesTest extends AbstractTestMessages
{
    @Test
    public void testVirtualhostCreated()
    {
        final String name = "test";
        final String attributes = "{\"type\": \"JSON\"}";
        _logMessage = VirtualHostMessages.CREATE(name, String.valueOf(Outcome.SUCCESS), attributes);
        final List<Object> log = performLog();

        final String[] expected = {"Create : \"", name, "\" : ", String.valueOf(Outcome.SUCCESS), " : ", attributes};

        validateLogMessage(log, "VHT-1001", expected);
    }

    @Test
    public void testVirtualhostClosed()
    {
        final String name = "test";
        _logMessage = VirtualHostMessages.CLOSE(name);
        final List<Object> log = performLog();

        final String[] expected = {"Close : \"", name, "\""};

        validateLogMessage(log, "VHT-1002", expected);
    }
}
