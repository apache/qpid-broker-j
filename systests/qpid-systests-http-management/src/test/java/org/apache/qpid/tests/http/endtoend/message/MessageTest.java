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

package org.apache.qpid.tests.http.endtoend.message;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.typeCompatibleWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig
public class MessageTest extends HttpTestBase
{
    private static final String QUEUE_NAME = "myqueue";
    private static final TypeReference<List<Map<String, Object>>> LIST_MAP_TYPE_REF =
            new TypeReference<List<Map<String, Object>>>()
            {
            };
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
            new TypeReference<Map<String, Object>>()
            {
            };
    private static final TypeReference<List<Object>> LIST_TYPE_REF =
            new TypeReference<List<Object>>()
            {
            };

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
        getHelper().setTls(true);
    }

    @Test
    public void getJmsMessageWithProperty() throws Exception
    {
        final String messageProperty = "myProp";
        final String messagePropertyValue = "myValue";

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            Message message = session.createMessage();
            message.setStringProperty(messageProperty, messagePropertyValue);
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders",
                                                                                           Boolean.TRUE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));

        Map<String, Object> message = messages.get(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) message.get("headers");
        assertThat(headers.get(messageProperty), is(equalTo(messagePropertyValue)));
    }

    @Test
    public void getJmsMessageWithGroupId() throws Exception
    {
        final String groupIdValue = "mygroup";

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            Message message = session.createMessage();
            message.setStringProperty("JMSXGroupID", groupIdValue);
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders",
                                                                                           Boolean.FALSE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));

        Map<String, Object> message = messages.get(0);

        assertThat(message.get("groupId"), is(groupIdValue));
    }


    @Test
    public void getAcquiredMessage() throws Exception
    {
        Connection connection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(QUEUE_NAME);

            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            Message jmsMessage = session.createMessage();
            producer.send(jmsMessage);

            List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                      Collections.singletonMap("includeHeaders",
                                                                                               Boolean.FALSE),
                                                                      LIST_MAP_TYPE_REF, SC_OK);
            assertThat(messages.size(), is(equalTo(1)));
            Map<String, Object> message = messages.get(0);

            assertThat(message.get("deliveredToConsumerId"), is(nullValue()));
            connection.start();
            MessageConsumer consumer = session.createConsumer(queue);
            jmsMessage = consumer.receive(getReceiveTimeout());
            assertThat(jmsMessage, is(notNullValue()));

            messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                      Collections.singletonMap("includeHeaders",
                                                                                               Boolean.FALSE),
                                                                      LIST_MAP_TYPE_REF, SC_OK);
            assertThat(messages.size(), is(equalTo(1)));

            message = messages.get(0);

            assertThat(message.get("deliveredToConsumerId"), is(notNullValue()));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void getJmsMapMessage() throws Exception
    {
        final String mapKey = "key";
        final String mapKeyValue = "value";

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            MapMessage message = session.createMapMessage();
            message.setString(mapKey, mapKeyValue);
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders",
                                                                                           Boolean.TRUE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));
        Map<String, Object> message = messages.get(0);
        int messageId = (int) message.get("id");

        Map<String, Object> contentParams = new HashMap<>();
        contentParams.put("messageId", messageId);
        contentParams.put("returnJson", Boolean.TRUE);

        Map<String, Object> content = getHelper().postJson("queue/myqueue/getMessageContent",
                                                           contentParams,
                                                           MAP_TYPE_REF, SC_OK);
        assertThat(content.size(), is(equalTo(1)));
        assertThat(content.get(mapKey), is(equalTo(mapKeyValue)));
    }

    @Test
    public void getJmsStreamMessage() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            StreamMessage message = session.createStreamMessage();
            message.writeLong(Long.MAX_VALUE);
            message.writeBoolean(true);
            message.writeString("Hello World");
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders",
                                                                                           Boolean.TRUE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));
        Map<String, Object> message = messages.get(0);
        int messageId = (int) message.get("id");

        Map<String, Object> contentParams = new HashMap<>();
        contentParams.put("messageId", messageId);
        contentParams.put("returnJson", Boolean.TRUE);

        List<Object> content = getHelper().postJson("queue/myqueue/getMessageContent",
                                                    contentParams,
                                                    LIST_TYPE_REF, SC_OK);
        assertThat(content.size(), is(equalTo(3)));
        assertThat(content.get(0), is(equalTo(Long.MAX_VALUE)));
        assertThat(content.get(1), is(equalTo(Boolean.TRUE)));
        assertThat(content.get(2), is(equalTo("Hello World")));
    }

    @Test
    public void getJmsBytesMessage() throws Exception
    {
        final byte[] content = new byte[512];
        IntStream.range(0, content.length).forEachOrdered(i -> content[i] = (byte) (i % 256));

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(content);
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders",
                                                                                           Boolean.TRUE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));
        Map<String, Object> message = messages.get(0);
        int messageId = (int) message.get("id");

        byte[] receivedContent = getHelper().getBytes(String.format(
                "queue/myqueue/getMessageContent?messageId=%d", messageId));

        assumeThat("AMQP1.0 messages return the AMQP type",
                   getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        assertThat(receivedContent, is(equalTo(content)));
    }

    @Test
    public void publishEmptyMessage() throws Exception
    {
        Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("address", QUEUE_NAME);

        getHelper().submitRequest("virtualhost/publishMessage",
                                  "POST",
                                  Collections.singletonMap("message", messageBody),
                                  SC_OK);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertThat(message, is(notNullValue()));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void publishMessageApplicationHeaders() throws Exception
    {
        final String stringPropValue = "mystring";
        final String longStringPropValue = Strings.repeat("*", 256);
        final Map<String, Object> headers = new HashMap<>();
        headers.put("stringprop", stringPropValue);
        headers.put("longstringprop", longStringPropValue);
        headers.put("intprop", Integer.MIN_VALUE);
        headers.put("longprop", Long.MAX_VALUE);
        headers.put("boolprop", Boolean.TRUE);

        final Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("address", QUEUE_NAME);
        messageBody.put("headers", headers);

        getHelper().submitRequest("virtualhost/publishMessage",
                                  "POST",
                                  Collections.singletonMap("message", messageBody),
                                  SC_OK);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertThat(message, is(notNullValue()));
            assertThat(message.getStringProperty("stringprop"), is(equalTo(stringPropValue)));
            assertThat(message.getIntProperty("intprop"), is(equalTo(Integer.MIN_VALUE)));
            assertThat(message.getLongProperty("longprop"), is(equalTo(Long.MAX_VALUE)));
            assertThat(message.getBooleanProperty("boolprop"), is(equalTo(Boolean.TRUE)));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void publishMessageHeaders() throws Exception
    {
        final String messageId = "ID:" + UUID.randomUUID().toString();
        final long expiration = TimeUnit.DAYS.toMillis(1) + System.currentTimeMillis();

        Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("address", QUEUE_NAME);
        messageBody.put("messageId", messageId);
        messageBody.put("expiration", expiration);

        getHelper().submitRequest("virtualhost/publishMessage",
                                  "POST",
                                  Collections.singletonMap("message", messageBody),
                                  SC_OK);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertThat(message, is(notNullValue()));
            assertThat(message.getJMSMessageID(), is(equalTo(messageId)));
            assertThat(message.getJMSExpiration(), is(greaterThanOrEqualTo(expiration)));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void publishStringMessage() throws Exception
    {
        final String content = "Hello world";
        TextMessage message = publishMessageWithContent(content, TextMessage.class);
        assertThat("Unexpected message content", message.getText(), is(equalTo(content)));
    }

    @Test
    public void publishMapMessage() throws Exception
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
            assertThat("Unexpected map content for key : " + key, message.getObject(key), is(equalTo(content.get(key))));
            entryCount++;
        }
        assertThat("Unexpected number of key/value pairs in map message", entryCount, is(equalTo(content.size())));
    }

    @Test
    public void publishListMessage() throws Exception
    {
        final List<Object> content = new ArrayList<>();
        content.add("astring");
        content.add(Integer.MIN_VALUE);
        content.add(Long.MAX_VALUE);
        content.add(null);
        StreamMessage message = publishMessageWithContent(content, StreamMessage.class);
        assertThat(message.readString(), is(equalTo("astring")));
        assertThat(message.readInt(), is(equalTo((Integer.MIN_VALUE))));
        assertThat(message.readLong(), is(equalTo(Long.MAX_VALUE)));
        assertThat(message.readObject(), is(nullValue()));
    }

    private <M extends Message> M publishMessageWithContent(final Object content, final Class<M> expectedMessageClass)
            throws Exception
    {
        Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("address", QUEUE_NAME);
        messageBody.put("content", content);

        getHelper().submitRequest("virtualhost/publishMessage",
                                  "POST",
                                  Collections.singletonMap("message", messageBody),
                                  SC_OK);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);

            @SuppressWarnings("unchecked")
            M message = (M) consumer.receive(getReceiveTimeout());
            assertThat(message, is(notNullValue()));
            assertThat(message.getClass(), is(typeCompatibleWith(expectedMessageClass)));
            return message;
        }
        finally
        {
            connection.close();
        }
    }
}
