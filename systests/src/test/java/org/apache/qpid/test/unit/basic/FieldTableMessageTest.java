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

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.framing.AMQFrameDecodingException;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.FieldTableFactory;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class FieldTableMessageTest extends QpidBrokerTestCase implements MessageListener
{
    private static final Logger _logger = LoggerFactory.getLogger(FieldTableMessageTest.class);

    private Connection _connection;
    private Destination _destination;
    private Session _session;
    private final ArrayList<BytesMessage> received = new ArrayList<>();
    private FieldTable _expected;
    private int _count = 10;
    private CountDownLatch _waitForCompletion;

    protected void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _destination = createTestQueue(_session);
        // set up a slow consumer
        _session.createConsumer(_destination).setMessageListener(this);
        _connection.start();

        // _expected = new FieldTableTest().load("FieldTableTest2.properties");
        _expected = load();
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    private FieldTable load() throws IOException
    {
        FieldTable result = FieldTableFactory.newFieldTable();
        result.setLong("one", 1L);
        result.setLong("two", 2L);
        result.setLong("three", 3L);
        result.setLong("four", 4L);
        result.setLong("five", 5L);

        return result;
    }

    public void test() throws Exception
    {
        int count = _count;
        _waitForCompletion = new CountDownLatch(_count);
        send(count);
        _waitForCompletion.await(20, TimeUnit.SECONDS);
        check();
        _logger.info("Completed without failure");
        _connection.close();
    }

    void send(int count) throws JMSException, IOException
    {
        // create a publisher
        MessageProducer producer = _session.createProducer(_destination);
        for (int i = 0; i < count; i++)
        {
            BytesMessage msg = _session.createBytesMessage();
            msg.writeBytes(_expected.getDataAsBytes());
            producer.send(msg);
        }
    }


    void check() throws JMSException, AMQFrameDecodingException, IOException
    {
        for (BytesMessage bytesMessage : received)
        {
            final long bodyLength = bytesMessage.getBodyLength();
            byte[] data = new byte[(int) bodyLength];
            bytesMessage.readBytes(data);
            FieldTable actual = new FieldTable(QpidByteBuffer.wrap(data));
            for (String key : _expected.keys())
            {
                assertEquals("Values for " + key + " did not match", _expected.getObject(key), actual.getObject(key));
            }
        }
    }

    public void onMessage(Message message)
    {
        synchronized (received)
        {
            received.add((BytesMessage) message);
            _waitForCompletion.countDown();
        }
    }


}
