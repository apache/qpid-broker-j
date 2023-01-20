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
package org.apache.qpid.server.logging.subjects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.UnitTestMessageLogger;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;


/**
 * Abstract Test for LogSubject testing
 * Includes common validation code and two common tests.
 *
 * Each test class sets up the LogSubject and contains details of how to
 * validate this class then performs a log statement with logging enabled and
 * logging disabled.
 *
 * The resulting log file is then validated.
 *
 */
public abstract class AbstractTestLogSubject extends UnitTestBase
{
    protected LogSubject _subject = null;

    protected List<Object> performLog(final boolean statusUpdatesEnabled)
    {
        if (_subject == null)
        {
            throw new NullPointerException("LogSubject has not been set");
        }

        final UnitTestMessageLogger logger = new UnitTestMessageLogger(statusUpdatesEnabled);
        final EventLogger eventLogger = new EventLogger(logger);

        eventLogger.message(_subject, new LogMessage()
        {
            @Override
            public String toString()
            {
                return "<Log Message>";
            }

            @Override
            public String getLogHierarchy()
            {
                return "test.hierarchy";
            }
        });

        return logger.getLogMessages();
    }

    /**
     * Verify that the connection section has the expected items
     *
     * @param connectionID - The connection id (int) to check for
     * @param user         - the Connected username
     * @param ipString     - the ipString/hostname
     * @param vhost        - the virtualhost that the user connected to.
     * @param message      - the message these values should appear in.
     */
    protected void verifyConnection(final long connectionID,
                                    final String user,
                                    final String ipString,
                                    final String vhost,
                                    final String message)
    {
        // This should return us MockProtocolSessionUser@null/test
        final String connectionSlice = getSlice("con:" + connectionID, message);

        assertNotNull(connectionSlice, "Unable to find connection 'con:" + connectionID + "' in '" + message + "'");

        // Extract the userName
        final String[] userNameParts = connectionSlice.split("@");

        assertEquals(2, (long) userNameParts.length,
                "Unable to split Username from rest of Connection:" + connectionSlice);

        assertEquals(userNameParts[0], user, "Username not as expected");

        // Extract IP.
        // The connection will be of the format - guest@/127.0.0.1:1/test
        // and so our userNamePart will be '/127.0.0.1:1/test'
        final String[] ipParts = userNameParts[1].split("/");

        // We will have three sections
        assertEquals(3, (long) ipParts.length, "Unable to split IP from rest of Connection:" +
                userNameParts[1] + " in '" + message + "'");

        // We need to skip the first '/' split will be empty so validate 1 as IP
        assertEquals(ipString, ipParts[1], "IP not as expected");

        //Finally check vhost which is section 2
        assertEquals(vhost, ipParts[2], "Virtualhost name not as expected.");
    }

    /**
     * Verify that the RoutingKey is present in the provided message.
     *
     * @param message    The message to check
     * @param routingKey The routing key to check against
     */
    protected void verifyRoutingKey(final String message, final String routingKey)
    {
        final String routingKeySlice = getSlice("rk", message);

        assertNotNull(routingKeySlice, "Routing Key not found:" + message);

        assertEquals(routingKey, routingKeySlice, "Routing key not correct");
    }

    /**
     * Verify that the given Queue's name exists in the provided message
     *
     * @param message The message to check
     * @param queue   The queue to check against
     */
    protected void verifyQueue(final String message, final Queue<?> queue)
    {
        final String queueSlice = getSlice("qu", message);

        assertNotNull(queueSlice, "Queue not found:" + message);

        assertEquals(queue.getName(), queueSlice, "Queue name not correct");
    }

    /**
     * Verify that the given exchange (name and type) are present in the
     * provided message.
     *
     * @param message  The message to check
     * @param exchange the exchange to check against
     */
    protected void verifyExchange(final String message, final Exchange<?> exchange)
    {
        final String exchangeSlice = getSlice("ex", message);

        assertNotNull(exchangeSlice, "Exchange not found:" + message);

        final String[] exchangeParts = exchangeSlice.split("/");

        assertEquals(2, (long) exchangeParts.length, "Exchange should be in two parts ex(type/name)");

        assertEquals(exchange.getType(), exchangeParts[0], "Exchange type not correct");

        assertEquals(exchange.getName(), exchangeParts[1], "Exchange name not correct");
    }

    /**
     * Verify that a VirtualHost with the given name appears in the given
     * message.
     *
     * @param message the message to search
     * @param vhost   the vhostName to check against
     */
    static public void verifyVirtualHost(final String message, final VirtualHost<?> vhost)
    {
        final String vhostSlice = getSlice("vh", message);

        assertNotNull(vhostSlice, "Virtualhost not found:" + message);

        assertEquals("/" + vhost.getName(), vhostSlice, "Virtualhost not correct");
    }

    /**
     * Parse the log message and return the slice according to the following:
     * Given Example:
     * con:1(guest@127.0.0.1/test)/ch:2/ex(amq.direct)/qu(myQueue)/rk(myQueue)
     *
     * Each item (except channel) is of the format <key>(<values>)
     *
     * So Given an ID to slice on:
     * con:1 - Connection 1
     * ex - exchange
     * qu - queue
     * rk - routing key
     *
     * @param sliceID the slice to locate
     * @param message the message to search in
     *
     * @return the slice if found otherwise null is returned
     */
    static public String getSlice(final String sliceID, final String message)
    {
        final int indexOfSlice = message.indexOf(sliceID + "(");

        if (indexOfSlice == -1)
        {
            return null;
        }

        final int endIndex = message.indexOf(')', indexOfSlice);

        if (endIndex == -1)
        {
            return null;
        }

        return message.substring(indexOfSlice + 1 + sliceID.length(), endIndex);
    }

    /**
     * Test that when Logging occurs a single log statement is provided
     *
     */
    @Test
    public void testEnabled()
    {
        final List<Object> logs = performLog(true);

        assertEquals(1, (long) logs.size(), "Log has incorrect message count");

        validateLogStatement(String.valueOf(logs.get(0)));
    }

    /**
     * Call to the individual tests to validate the message is formatted as
     * expected
     *
     * @param message the message whose format needs validation
     */
    protected abstract void validateLogStatement(final String message);

    /**
     * Ensure that when status updates are off this does not perform logging
     */
    @Test
    public void testDisabled()
    {
        final List<Object> logs = performLog(false);

        assertEquals(0, (long) logs.size(), "Log has incorrect message count");
    }
}
