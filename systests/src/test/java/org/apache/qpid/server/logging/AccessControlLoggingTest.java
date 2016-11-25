/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.logging;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.security.acl.AbstractACLTestCase;

/**
 * ACL version 2/3 file testing to verify that ACL actor logging works correctly.
 *
 * This suite of tests validate that the AccessControl messages occur correctly
 * and according to the following format:
 *
 * <pre>
 * ACL-1001 : Allowed Operation Object {PROPERTIES}
 * ACL-1002 : Denied Operation Object {PROPERTIES}
 * </pre>
 */
public class AccessControlLoggingTest extends AbstractTestLogging
{
    private static final String ACL_LOG_PREFIX = "ACL-";
    private static final String USER = "client";
    private static final String PASS = "guest";
    private Connection _connection;
    private Session _session;

    public void setUp() throws Exception
    {
        // Write out ACL for this test
        AbstractACLTestCase.writeACLFileUtil(this, "ACL ALLOW client ACCESS VIRTUALHOST",
                "ACL ALLOW client CREATE QUEUE name='allow'",
                "ACL ALLOW-LOG client CREATE QUEUE name='allow-log'",
                "ACL DENY client CREATE QUEUE name='deny'",
                "ACL DENY-LOG client CREATE QUEUE name='deny-log'",
                "ACL ALLOW client PUBLISH EXCHANGE name='' routingkey='$management");

        super.setUp();

        _connection = getConnection(USER, PASS);
        _session = _connection.createSession(true, Session.SESSION_TRANSACTED);
        _connection.start();

    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            _connection.close();
            super.tearDown();
        }
        catch (JMSException e)
        {
            //we're throwing this away as it can happen in this test as the state manager remembers exceptions
            //that we provoked with authentication failures, where the test passes - we can ignore on con close
        }
    }

    /**
     * Test that {@code allow} ACL entries do not log anything.
     */
    public void testAllow() throws Exception
	{
        String name = "allow";
        boolean autoDelete = false;
        boolean durable = false;
        boolean exclusive = false;
        createQueue(name, autoDelete, durable, exclusive);

        List<String> matches = findMatches(ACL_LOG_PREFIX + 1001);

        assertTrue("Should be no ACL log messages", matches.isEmpty());
    }

    protected void createQueue(final String name,
                               final boolean autoDelete,
                               final boolean durable, final boolean exclusive)
            throws JMSException
    {
        Map<String,Object> attributes = new LinkedHashMap<>();
        attributes.put(ConfiguredObject.NAME, name);
        attributes.put(ConfiguredObject.DURABLE, durable);
        LifetimePolicy lifetimePolicy;
        ExclusivityPolicy exclusivityPolicy;

        if (exclusive)
        {
            lifetimePolicy = autoDelete
                    ? LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS
                    : durable ? LifetimePolicy.PERMANENT : LifetimePolicy.DELETE_ON_CONNECTION_CLOSE;
            exclusivityPolicy = durable ? ExclusivityPolicy.CONTAINER : ExclusivityPolicy.CONNECTION;
        }
        else
        {
            lifetimePolicy = autoDelete ? LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS : LifetimePolicy.PERMANENT;
            exclusivityPolicy = ExclusivityPolicy.NONE;
        }

        attributes.put(Queue.EXCLUSIVE, exclusivityPolicy.toString());
        attributes.put(Queue.LIFETIME_POLICY, lifetimePolicy.toString());
        createEntityUsingAmqpManagement(name, _session, "org.apache.qpid.Queue", attributes);
    }

    /**
     * Test that {@code allow-log} ACL entries log correctly.
     */
    public void testAllowLog() throws Exception
    {
        createQueue("allow-log", false, false, false);

        List<String> matches = findMatches(ACL_LOG_PREFIX + 1001);

        assertEquals("Should only be one ACL log message", 1, matches.size());

        String log = getLogMessage(matches, 0);
        String actor = fromActor(log);
        String subject = fromSubject(log);
        String message = getMessageString(fromMessage(log));

        validateMessageID(ACL_LOG_PREFIX + 1001, log);

        assertTrue("Actor " + actor + " should contain the user identity: " + USER, actor.contains(USER));
        assertTrue("Subject should be empty", subject.length() == 0);
        assertTrue("Message should start with 'Allowed'", message.startsWith("Allowed"));
        assertTrue("Message should contain 'Create Queue'", message.contains("Create Queue"));
        assertTrue("Message should have contained the queue name", message.contains("allow-log"));
    }

    /**
     * Test that {@code deny-log} ACL entries log correctly.
     */
    public void testDenyLog() throws Exception
    {
        createQueue("deny-log", false, false, false);

        List<String> matches = findMatches(ACL_LOG_PREFIX + 1002);

        assertEquals("Should only be one ACL log message", 1, matches.size());

        String log = getLogMessage(matches, 0);
        String actor = fromActor(log);
        String subject = fromSubject(log);
        String message = getMessageString(fromMessage(log));

        validateMessageID(ACL_LOG_PREFIX + 1002, log);

        assertTrue("Actor " + actor + " should contain the user identity: " + USER, actor.contains(USER));
        assertTrue("Subject should be empty", subject.length() == 0);
        assertTrue("Message should start with 'Denied'", message.startsWith("Denied"));
        assertTrue("Message should contain 'Create Queue'", message.contains("Create Queue"));
        assertTrue("Message should have contained the queue name", message.contains("deny-log"));
    }

    /**
     * Test that {@code deny} ACL entries do not log anything.
     */
    public void testDeny() throws Exception
    {
        createQueue("deny", false, false, false);

        List<String> matches = findMatches(ACL_LOG_PREFIX + 1002);

        assertTrue("Should be no ACL log messages", matches.isEmpty());
    }
}
