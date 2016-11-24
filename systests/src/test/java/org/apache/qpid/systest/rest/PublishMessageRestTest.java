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
package org.apache.qpid.systest.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Strings;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class PublishMessageRestTest extends QpidRestTestCase
{
    private Connection _connection;
    private Session _session;
    private String _queueName;
    private MessageConsumer _consumer;
    private String _publishMessageOpUrl;
    private String _queueUrl;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _connection = getConnection();
        _connection.start();

        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _queueName = getTestQueueName();
        Destination queue = createTestQueue(_session);

        _consumer = _session.createConsumer(queue);

        _publishMessageOpUrl = String.format("virtualhost/%s/%s/publishMessage", TEST1_VIRTUALHOST, TEST1_VIRTUALHOST);
        _queueUrl = String.format("queue/%s/%s/", TEST1_VIRTUALHOST, TEST1_VIRTUALHOST);
    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        getDefaultBrokerConfiguration().setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT,
                                                           HttpPort.ALLOW_CONFIDENTIAL_OPERATIONS_ON_INSECURE_CHANNELS,
                                                           true);
    }

    public void testPublishMinimalEmptyMessage() throws Exception
    {
        Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("address", _queueName);

        getRestTestHelper().submitRequest(_publishMessageOpUrl,
                                          "POST",
                                          Collections.singletonMap("message", messageBody), HttpServletResponse.SC_OK);

        Message message = _consumer.receive(getLongReceiveTimeout());
        assertNotNull("Expected message not received", message);
        assertNull("Unexpected JMSMessageID", message.getJMSMessageID());
        assertNull("Unexpected JMSCorrelationID", message.getJMSCorrelationID());
        assertEquals("Unexpected JMSExpiration", 0, message.getJMSExpiration());
        assertNotSame("Unexpected JMSTimestamp", 0, message.getJMSTimestamp());

        // remove any JMSX properties which may be added by the client library
        ArrayList propertyNames = new ArrayList(Collections.list(message.getPropertyNames()));
        Iterator iter = propertyNames.iterator();
        while(iter.hasNext())
        {
            if(iter.next().toString().startsWith("JMSX"))
            {
                iter.remove();
            }
        }
        assertTrue("Unexpected number of mesage properties: " + propertyNames, propertyNames.isEmpty());
    }

    public void testPublishMessageWithPropertiesAndHeaders() throws Exception
    {
        final String messageId = "ID:" + UUID.randomUUID().toString();
        final long tomorrow = TimeUnit.DAYS.toMillis(1) + System.currentTimeMillis();
        final Map<String, Object> headers =  new HashMap<>();
        headers.put("stringprop", "mystring");
        headers.put("longstringprop", Strings.repeat("*", 256));
        headers.put("intprop", Integer.MIN_VALUE);
        headers.put("longprop", Long.MAX_VALUE);
        final Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("messageId", messageId);
        messageBody.put("address", _queueName);
        messageBody.put("expiration", tomorrow);
        messageBody.put("headers", headers);

        getRestTestHelper().submitRequest(_publishMessageOpUrl,
                                          "POST",
                                          Collections.singletonMap("message", messageBody), HttpServletResponse.SC_OK);

        Message message = _consumer.receive(getLongReceiveTimeout());
        assertNotNull("Expected message not received", message);
        final String jmsMessageID = message.getJMSMessageID();
        assertEquals("Unexpected JMSMessageID", messageId, jmsMessageID);
        assertEquals("Unexpected JMSExpiration", tomorrow, message.getJMSExpiration());

        final Enumeration propertyEnumeration = message.getPropertyNames();
        int count = 0;
        while(propertyEnumeration.hasMoreElements())
        {
            String key = (String) propertyEnumeration.nextElement();
            assertEquals("Unexpected property value fo key : " + key, headers.get(key), message.getObjectProperty(key));
            count++;
        }
        assertEquals("Unexpected number of properties", headers.size(), count);
    }

    private void expectPublishFailure(final Map<String, Object> headers, final int responseCode) throws IOException
    {
        final Map<String, Object> messageBody = Collections.<String, Object>singletonMap("headers", headers);

        getRestTestHelper().submitRequest(_publishMessageOpUrl, "POST",
                                          Collections.singletonMap("message", messageBody),
                                          responseCode);
    }

    public void testPublishStringMessage() throws Exception
    {
        final String content = "Hello world";
        TextMessage message = publishMessageWithContent(content, TextMessage.class);
        assertEquals("Unexpected message content", content, message.getText());
    }

    public void testPublishMapMessage() throws Exception
    {
        final Map<String, Object> content = new HashMap<>();
        content.put("key1", "astring");
        content.put("key2", Integer.MIN_VALUE);
        content.put("key3", Long.MAX_VALUE);
        content.put("key4", null);
        MapMessage message = publishMessageWithContent(content, MapMessage.class);
        final Enumeration mapNames = message.getMapNames();
        int entryCount = 0;
        while(mapNames.hasMoreElements())
        {
            String key = (String) mapNames.nextElement();
            assertEquals("Unexpected map content for key : " + key, content.get(key), message.getObject(key));
            entryCount++;
        }
        assertEquals("Unexpected number of key/value pairs in map message", content.size(), entryCount);
    }

    public void testPublishRouting() throws Exception
    {
        final String queueName = UUID.randomUUID().toString();
        Map<String, Object> messageBody = Collections.<String, Object>singletonMap("address", queueName);

        int enqueues = getRestTestHelper().postJson(_publishMessageOpUrl,
                                                    Collections.singletonMap("message", messageBody),
                                                    Integer.class);
        assertEquals("Unexpected number of enqueues", 0, enqueues);

        getRestTestHelper().submitRequest(_queueUrl, "POST", Collections.singletonMap(Queue.NAME, queueName), HttpServletResponse.SC_CREATED);

        enqueues = getRestTestHelper().postJson(_publishMessageOpUrl,
                                                Collections.singletonMap("message", messageBody),
                                                Integer.class);


        assertEquals("Unexpected number of enqueues after queue creation", 1, enqueues);
    }

    private <M extends Message> M publishMessageWithContent(final Object content, final Class<M> expectedMessageClass) throws Exception
    {
        Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("address", _queueName);
        messageBody.put("content", content);

        getRestTestHelper().submitRequest(_publishMessageOpUrl,
                                          "POST",
                                          Collections.singletonMap("message", messageBody), HttpServletResponse.SC_OK);

        M message = (M) _consumer.receive(getLongReceiveTimeout());
        assertNotNull("Expected message not received", message);
        assertTrue(String.format("Unexpected message type. Expecting %s got %s", expectedMessageClass, message.getClass()),
                   expectedMessageClass.isAssignableFrom(message.getClass()));
        return message;
    }
}
