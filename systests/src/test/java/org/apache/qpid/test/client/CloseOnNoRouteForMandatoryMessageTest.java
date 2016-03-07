/*
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
 */
package org.apache.qpid.test.client;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 * Tests the broker's connection-closing behaviour when it receives an unroutable message
 * on a transactional session.
 *
 * @see ImmediateAndMandatoryPublishingTest for more general tests of mandatory and immediate publishing
 */
public class CloseOnNoRouteForMandatoryMessageTest extends QpidBrokerTestCase
{
    private static final Logger _logger = LoggerFactory.getLogger(CloseOnNoRouteForMandatoryMessageTest.class);

    private Connection _connection;
    private UnroutableMessageTestExceptionListener _testExceptionListener = new UnroutableMessageTestExceptionListener();

    public void testNoRoute_brokerClosesConnection() throws Exception
    {
        createConnectionWithCloseWhenNoRoute(true);

        Session transactedSession = _connection.createSession(true, Session.SESSION_TRANSACTED);
        String testQueueName = getTestQueueName();
        MessageProducer mandatoryProducer = ((AMQSession<?, ?>) transactedSession).createProducer(
                transactedSession.createQueue(testQueueName),
                true, // mandatory
                false); // immediate

        Message message = transactedSession.createMessage();
        mandatoryProducer.send(message);

        _testExceptionListener.assertReceivedNoRoute(testQueueName);

        try
        {
            transactedSession.commit();
            fail("Expected exception not thrown");
        }
        catch (IllegalStateException ise)
        {
            _logger.debug("Caught exception", ise);
            //The session was marked closed even before we had a chance to call commit on it
            assertTrue("ISE did not indicate closure", ise.getMessage().contains("closed"));
        }
    }

    public void testNoRouteForNonMandatoryMessage_brokerKeepsConnectionOpenAndCallsExceptionListener() throws Exception
    {
        createConnectionWithCloseWhenNoRoute(true);

        Session transactedSession = _connection.createSession(true, Session.SESSION_TRANSACTED);
        String testQueueName = getTestQueueName();
        MessageProducer nonMandatoryProducer = ((AMQSession<?, ?>) transactedSession).createProducer(
                transactedSession.createQueue(testQueueName),
                false, // mandatory
                false); // immediate

        Message message = transactedSession.createMessage();
        nonMandatoryProducer.send(message);

        // should succeed - the message is simply discarded
        transactedSession.commit();

        _testExceptionListener.assertNoException();
    }


    public void testNoRouteOnNonTransactionalSession_brokerKeepsConnectionOpenAndCallsExceptionListener() throws Exception
    {
        createConnectionWithCloseWhenNoRoute(true);

        Session nonTransactedSession = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String testQueueName = getTestQueueName();
        MessageProducer mandatoryProducer = ((AMQSession<?, ?>) nonTransactedSession).createProducer(
                nonTransactedSession.createQueue(testQueueName),
                true, // mandatory
                false); // immediate

        Message message = nonTransactedSession.createMessage();
        mandatoryProducer.send(message);

        _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, getTestQueueName());
    }

    public void testClientDisablesCloseOnNoRoute_brokerKeepsConnectionOpenAndCallsExceptionListener() throws Exception
    {
        createConnectionWithCloseWhenNoRoute(false);

        Session transactedSession = _connection.createSession(true, Session.SESSION_TRANSACTED);
        String testQueueName = getTestQueueName();
        MessageProducer mandatoryProducer = ((AMQSession<?, ?>) transactedSession).createProducer(
                transactedSession.createQueue(testQueueName),
                true, // mandatory
                false); // immediate

        Message message = transactedSession.createMessage();
        mandatoryProducer.send(message);
        transactedSession.commit();
        _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, getTestQueueName());
    }

    private void createConnectionWithCloseWhenNoRoute(boolean closeWhenNoRoute) throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put(ConnectionURL.OPTIONS_CLOSE_WHEN_NO_ROUTE, Boolean.toString(closeWhenNoRoute));
        _connection = getConnectionWithOptions(options);
        _connection.setExceptionListener(_testExceptionListener);
    }
}
