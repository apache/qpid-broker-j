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
package org.apache.qpid.test.unit.basic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class SessionStartTest extends QpidBrokerTestCase implements MessageListener
{
    private static final Logger _logger = LoggerFactory.getLogger(SessionStartTest.class);

    private Connection _connection;
    private Destination _destination;
    private Session _session;
    private CountDownLatch _countdownLatch;

    protected void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _destination = createTestQueue(_session);
        _connection.start();

        _session.createConsumer(_destination).setMessageListener(this);
        _countdownLatch = new CountDownLatch(1);
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    public void test() throws JMSException, InterruptedException
    {
        try
        {
            _session.createProducer(_destination).send(_session.createTextMessage("Message"));
            _logger.info("Message sent, waiting for response...");
            _countdownLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS);
        }
        finally
        {
            _session.close();
            _connection.close();
        }
    }

    public void onMessage(Message message)
    {
        _countdownLatch.countDown();
    }

}
