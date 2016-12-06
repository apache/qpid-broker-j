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
 *
 *
 */
package org.apache.qpid.test.unit.basic;


import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class LargeMessageTest extends QpidBrokerTestCase
{
    private static final Logger _logger = LoggerFactory.getLogger(LargeMessageTest.class);

    private Destination _destination;
    private Session _session;
    private Connection _connection;
    
    protected void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _destination = createTestQueue(_session);

        _connection.start();
    }

    protected void tearDown() throws Exception
    {
        _connection.close();
        super.tearDown();
    }

    public void test64kminus9() throws JMSException
    {
        checkLargeMessage((64 * 1024) - 9);
    }

    public void test64kminus8() throws JMSException
    {
        checkLargeMessage((64 * 1024)-8);
    }

    public void test64kminus7() throws JMSException
    {
        checkLargeMessage((64 * 1024)-7);
    }


    public void test64kplus1() throws JMSException
    {
        checkLargeMessage((64 * 1024) + 1);
    }

    public void test128kminus1() throws JMSException
    {
        checkLargeMessage((128 * 1024) - 1);
    }

    public void test128k() throws JMSException
    {
        checkLargeMessage(128 * 1024);
    }

    public void test128kplus1() throws JMSException
    {
        checkLargeMessage((128 * 1024) + 1);
    }

    // Testing larger messages

    public void test256k() throws JMSException
    {
        checkLargeMessage(256 * 1024);
    }

    public void test512k() throws JMSException
    {
        checkLargeMessage(512 * 1024);
    }

    public void test1024k() throws JMSException
    {
        checkLargeMessage(1024 * 1024);
    }

    protected void checkLargeMessage(int messageSize) throws JMSException
    {
            MessageConsumer consumer = _session.createConsumer(_destination);
            MessageProducer producer = _session.createProducer(_destination);
            _logger.info("Testing message of size:" + messageSize);

            String _messageText = buildLargeMessage(messageSize);

            _logger.debug("Message built");

            producer.send(_session.createTextMessage(_messageText));

            TextMessage result = (TextMessage) consumer.receive(10000);

            assertNotNull("Null message received", result);
            assertEquals("Message Size", _messageText.length(), result.getText().length());
            assertEquals("Message content differs", _messageText, result.getText());

    }

    private String buildLargeMessage(int size)
    {
        StringBuilder builder = new StringBuilder(size);

        char ch = 'a';

        for (int i = 1; i <= size; i++)
        {
            builder.append(ch);

            if ((i % 1000) == 0)
            {
                ch++;
                if (ch == ('z' + 1))
                {
                    ch = 'a';
                }
            }
        }

        return builder.toString();
    }

}
