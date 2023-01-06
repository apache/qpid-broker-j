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
 * Test CON Log Messages
 */
public class ConnectionMessagesTest extends AbstractTestMessages
{
    @Test
    public void testConnectionOpen_WithOptional()
    {
        final String port = "myport";
        final String local = "127.0.0.1:5672";
        final String protocolVersion = "0-8";
        final String clientID = "client";
        final String clientVersion = "1.2.3_4";
        final String clientProduct = "myProduct";

        _logMessage = ConnectionMessages.OPEN(port, local, protocolVersion, clientID, clientVersion, clientProduct , false, true, true, true);
        final List<Object> log = performLog();

        final String[] expected = {"Open",
                ": Destination", "myport(127.0.0.1:5672)",
                ": Protocol Version :", protocolVersion,
                ": Client ID", clientID,
                ": Client Version :", clientVersion,
                ": Client Product :", clientProduct};

        validateLogMessage(log, "CON-1001", expected);
    }

    @Test
    public void testConnectionOpen()
    {
        final String port = "myport";
        final String local = "127.0.0.1:5672";
        final String protocolVersion = "0-8";

        _logMessage = ConnectionMessages.OPEN(port, local, protocolVersion, null, null, null , false, false, false, false);
        final List<Object> log = performLog();

        final String[] expected = {"Open",
                ": Destination", "myport(127.0.0.1:5672)",
                ": Protocol Version :", protocolVersion};

        validateLogMessage(log, "CON-1001", expected);
    }

    @Test
    public void testSslConnectionOpen()
    {
        final String port = "myport";
        final String local = "127.0.0.1:5672";
        final String protocolVersion = "0-8";

        _logMessage = ConnectionMessages.OPEN(port, local, protocolVersion, null, null, null , true, false, false, false);
        final List<Object> log = performLog();

        final String[] expected = {"Open",
                ": Destination", "myport(127.0.0.1:5672)",
                ": Protocol Version :", protocolVersion,
                ": SSL"};

        validateLogMessage(log, "CON-1001", expected);
    }


    @Test
    public void testConnectionClose()
    {
        _logMessage = ConnectionMessages.CLOSE(null, false);
        final List<Object> log = performLog();

        final String[] expected = {"Close"};

        validateLogMessage(log, "CON-1002", expected);
    }

    @Test
    public void testConnectionCloseWithCause()
    {
        _logMessage = ConnectionMessages.CLOSE("Test", true);
        final List<Object> log = performLog();

        final String[] expected = {"Close : Test"};

        validateLogMessage(log, "CON-1002", expected);
    }
}
