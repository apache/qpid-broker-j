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

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.jms.Connection;

import org.apache.qpid.configuration.CommonProperties;

public class ConnectionLoggingTest extends AbstractTestLogging
{
    private static final String CONNECTION_PREFIX = "CON-";

    // No explicit startup configuration is required for this test
    // so no setUp() method

    /**
     * Description:
     * When a new connection is made to the broker this must be logged.
     *
     * Input:
     * 1. Running Broker
     * 2. Connecting client
     * Output:
     * <date> CON-1001 : Open ....
     *
     * Validation Steps:
     * 1. The CON ID is correct
     * 2. This is the first CON message for that Connection
     *
     * @throws Exception - if an error occurs
     */
    public void testConnectionOpen() throws Exception
    {
        assertLoggingNotYetOccured(CONNECTION_PREFIX);

        Connection connection = getConnection();
        String clientid = connection.getClientID();

        // Wait until opened
        waitForMessage("CON-1001");
        
        // Close the connection
        connection.close();

        // Wait to ensure that the desired message is logged
        waitForMessage("CON-1002");

        List<String> results = waitAndFindMatches("CON-1001");

        // MESSAGE [con:1(/127.0.0.1:46927)] CON-1001 : Open : Destination : amqp(127.0.0.1:15672) : Protocol Version : 0-10
        // MESSAGE [con:1(guest@/127.0.0.1:46927/test)] CON-1001 : Open : Destination : amqp(127.0.0.1:15672) : Protocol Version : 0-10 : Client ID : clientid : Client Version : 6.0.0-SNAPSHOT : Client Product : qpid
        // MESSAGE [con:1(guest@/127.0.0.1:46927/test)] CON-1002 : Close

        Map<Integer, List<String>> connectionData = splitResultsOnConnectionID(results);

        // Get the last Integer from keySet of the ConnectionData
        int connectionID = new TreeSet<>(connectionData.keySet()).last();

        //Use just the data from the last connection for the test
        results = connectionData.get(connectionID);

	    assertEquals("Unexpected CON-1001 messages count", 2, results.size());

        // validate the two CON-1001 messages.
        // MESSAGE [con:1(guest@/127.0.0.1:46927/test)] CON-1001 : Open : Destination : amqp(127.0.0.1:15672) : Protocol Version : 0-10 : Client ID : clientid : Client Version : 6.0.0-SNAPSHOT : Client Product : qpid
        validateConnectionOpen(results, 0,
                               true, getBrokerProtocol().getProtocolVersion(),
                               true, clientid,
                               true, CommonProperties.getReleaseVersion(),
                               true, CommonProperties.getProductName());

        // MESSAGE [con:1(/127.0.0.1:46927)] CON-1001 : Open : Destination : amqp(127.0.0.1:15672) : Protocol Version : 0-10
        validateConnectionOpen(results, 1,
                               true, getBrokerProtocol().getProtocolVersion(),
                               false, null,
                               false, null,
                               false, null);
    }
    
    private void validateConnectionOpen(List<String> results, int positionFromEnd,
                                        boolean protocolVersionPresent, final String protocolVersionValue,
                                        boolean clientIdOptionPresent, String clientIdValue,
                                        boolean clientVersionPresent, String clientVersionValue,
                                        boolean clientProductPresent, String clientProductValue)
    {
        String log = getLogMessageFromEnd(results, positionFromEnd);
        
        validateMessageID("CON-1001", log);

        String message = fromMessage(log);

        assertTrue("Destination not present", message.contains("Destination :"));

        assertEquals("unexpected Protocol Version option state",
                     protocolVersionPresent, message.contains("Protocol Version :"));
        if(protocolVersionPresent && protocolVersionValue != null)
        {
            assertTrue("Protocol Version value is not present: " + protocolVersionValue, message.contains(protocolVersionValue));
        }

        assertEquals("unexpected Client ID option state", clientIdOptionPresent, message.contains("Client ID :"));

        if(clientIdOptionPresent && clientIdValue != null)
        {
            assertTrue("Client ID value is not present: " + clientIdValue, message.contains(clientIdValue));
        }


        assertEquals("unexpected Client Version option state", clientVersionPresent, message.contains(
                "Client Version :"));

        if(clientVersionPresent && clientVersionValue != null && !isBroker10())
        {
            assertTrue("Client version value is not present: " + clientVersionValue, message.contains(
                    clientVersionValue));
        }

        assertEquals("unexpected Client Product option state", clientVersionPresent, message.contains(
                "Client Product :"));

        if(clientProductPresent && clientProductValue != null && !isBroker10())
        {
            assertTrue("Client product value is not present: " + clientProductValue, message.contains(
                    clientProductValue));
        }
    }

    /**
     * Description:
     * When a connected client closes the connection this will be logged as a CON-1002 message.
     * Input:
     *
     * 1. Running Broker
     * 2. Connected Client
     * Output:
     *
     * <date> CON-1002 : Close
     *
     * Validation Steps:
     * 3. The CON ID is correct
     * 4. This must be the last CON message for the Connection
     * 5. It must be preceded by a CON-1001 for this Connection
     */
    public void testConnectionClose() throws Exception
    {
        assertLoggingNotYetOccured(CONNECTION_PREFIX);

        Connection connection = getConnection();

        // Wait until opened
        waitForMessage("CON-1001");
        
        // Close the conneciton
        connection.close();

        // Wait to ensure that the desired message is logged
        waitForMessage("CON-1002");

        List<String> results = findMatches(CONNECTION_PREFIX);

        // Validation

        assertEquals("CON messages not logged:" + results.size(), 3, results.size());

        // Validate Close message occurs
        String log = getLogMessageFromEnd(results, 0);
        validateMessageID("CON-1002",log);
        assertTrue("Message does not end with close:" + log, log.endsWith("Close"));

        // Extract connection ID to validate there is a CON-1001 messasge for it
        final String logSubject = fromActor(log);
        int closeConnectionID = getConnectionID(logSubject);
        assertTrue("Could not get the connection id from CLOSE message: " + logSubject, closeConnectionID != -1);

        //Previous log message should be the open
        log = getLogMessageFromEnd(results, 1);
        //  MESSAGE [con:1(/127.0.0.1:52540)] CON-1001 : Open : Client ID : clientid : Protocol Version : 0-9
        validateMessageID("CON-1001",log);

        // Extract connection ID to validate it matches the CON-1002 messasge
        int openConnectionID = getConnectionID(fromActor(log));
        assertTrue("Could not find connection id in OPEN", openConnectionID != -1);
        
        // Check connection ids match
        assertEquals("Connection IDs do not match", closeConnectionID, openConnectionID);
    }
}
