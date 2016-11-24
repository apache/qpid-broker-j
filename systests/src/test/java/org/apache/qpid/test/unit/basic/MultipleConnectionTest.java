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
 *
 */
package org.apache.qpid.test.unit.basic;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class MultipleConnectionTest extends QpidBrokerTestCase
{
    private static final Logger _logger = LoggerFactory.getLogger(MultipleConnectionTest.class);


    private class Receiver
    {
        private Connection _connection;
        private Session[] _sessions;
        private MessageCounter[] _counters;

        Receiver(Destination dest, int sessions) throws Exception
        {
            this(getConnection("guest", "guest"), dest, sessions);
        }

        Receiver(Connection connection, Destination dest, int sessions) throws Exception
        {
            _connection = connection;
            _sessions = new Session[sessions];
            _counters = new MessageCounter[sessions];
            for (int i = 0; i < sessions; i++)
            {
                _sessions[i] = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                _counters[i] = new MessageCounter(_sessions[i].toString());
                _sessions[i].createConsumer(dest).setMessageListener(_counters[i]);
            }

            _connection.start();
        }

        void close() throws JMSException
        {
            _connection.close();
        }

        public MessageCounter[] getCounters()
        {
            return _counters;
        }
    }

    private class Publisher
    {
        private Connection _connection;
        private Session _session;
        private MessageProducer _producer;

        Publisher(Destination dest) throws Exception
        {
            this(getConnection("guest", "guest"), dest);
        }

        Publisher(Connection connection, Destination dest) throws Exception
        {
            _connection = connection;
            _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _producer = _session.createProducer(dest);
        }

        void send(String msg) throws JMSException
        {
            _producer.send(_session.createTextMessage(msg));
        }

        void close() throws JMSException
        {
            _connection.close();
        }
    }

    private static class MessageCounter implements MessageListener
    {
        private final String _name;
        private int _count;

        MessageCounter(String name)
        {
            _name = name;
        }

        public synchronized void onMessage(Message message)
        {
            _count++;
            notify();
        }

        synchronized boolean waitUntil(int expected, long maxWait) throws InterruptedException
        {
            long start = System.currentTimeMillis();
            while (expected > _count)
            {
                long timeLeft = maxWait - timeSince(start);
                if (timeLeft <= 0)
                {
                    break;
                }

                wait(timeLeft);
            }

            return expected <= _count;
        }

        private long timeSince(long start)
        {
            return System.currentTimeMillis() - start;
        }

        public synchronized String toString()
        {
            return _name + ": " + _count;
        }
    }

    private static void waitForCompletion(int expected, long wait, Receiver[] receivers) throws InterruptedException
    {
        for (int i = 0; i < receivers.length; i++)
        {
            waitForCompletion(expected, wait, receivers[i].getCounters());
        }
    }

    private static void waitForCompletion(int expected, long wait, MessageCounter[] counters) throws InterruptedException
    {
        for (int i = 0; i < counters.length; i++)
        {
            if (!counters[i].waitUntil(expected, wait))
            {
                throw new RuntimeException("Expected: " + expected + " got " + counters[i]);
            }
        }
    }

    public void test() throws Exception
    {
        int messages = 10;

        Topic topic = createTopic(getConnection(), getTestName());

        Receiver[] receivers = new Receiver[] { new Receiver(topic, 2), new Receiver(topic, 14) };

        Publisher publisher = new Publisher(topic);
        for (int i = 0; i < messages; i++)
        {
            publisher.send("Message " + (i + 1));
        }

        try
        {
            waitForCompletion(messages, 5000, receivers);
            _logger.info("All receivers received all expected messages");
        }
        finally
        {
            publisher.close();
            for (int i = 0; i < receivers.length; i++)
            {
                receivers[i].close();
            }
        }
    }
}
